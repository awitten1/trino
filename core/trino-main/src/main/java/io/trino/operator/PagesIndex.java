/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.geospatial.Rectangle;
import io.trino.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.trino.operator.join.JoinHashSupplier;
import io.trino.operator.join.JoinUtils;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.LookupSourceSupplier;
import io.trino.operator.join.SortOperator;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinCompiler.LookupSourceSupplierFactory;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.HashArraySizeSupplier.defaultHashArraySizeSupplier;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.operator.SyntheticAddress.encodeSyntheticAddress;
import static io.trino.operator.join.JoinUtils.getSingleBigintJoinChannel;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static java.util.Objects.requireNonNull;

/**
 * PagesIndex a low-level data structure which contains the address of every value position of every channel.
 * This data structure is not general purpose and is designed for a few specific uses:
 * <ul>
 * <li>Sort via the {@link #sort} method</li>
 * <li>Hash build via the {@link #createLookupSourceSupplier} method</li>
 * <li>Positional output via the {@link #appendTo} method</li>
 * </ul>
 */
public class PagesIndex
        implements Swapper
{
    private static final int INSTANCE_SIZE = instanceSize(PagesIndex.class);
    private static final Logger log = Logger.get(PagesIndex.class);

    private final OrderingCompiler orderingCompiler;
    private final JoinCompiler joinCompiler;
    private final BlockTypeOperators blockTypeOperators;

    private final List<Type> types;
    private final LongArrayList valueAddresses;
    private final ObjectArrayList<Block>[] channels;
    private final IntArrayList positionCounts;
    private final boolean eagerCompact;

    private int modificationCount; // may overflow, doesn't matter
    private int pageCount;
    private int nextBlockToCompact;
    private int positionCount;
    private long pagesMemorySize;
    private long estimatedSize;
    private PagesIndexOrdering pagesIndexOrdering;
    private final List<Integer> batchEndingPosition;
    private int pagesBatchSize;
    private int pagesBatchIter;
    private final Map<String, List<Integer>> pagesBySplit = new HashMap<>();

    private final Map<String, Map<String, List<Page>>> seqNumToPageBySplit = new HashMap<>();

    private PagesIndex(
            OrderingCompiler orderingCompiler,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            List<Type> types,
            int expectedPositions,
            boolean eagerCompact)
    {
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.valueAddresses = new LongArrayList(expectedPositions);
        this.eagerCompact = eagerCompact;
        this.pagesBatchSize = 0;
        this.pagesBatchIter = 0;

        //noinspection unchecked
        channels = (ObjectArrayList<Block>[]) new ObjectArrayList[types.size()];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = ObjectArrayList.wrap(new Block[1024], 0);
        }

        positionCounts = new IntArrayList(1024);

        estimatedSize = calculateEstimatedSize();
        pagesIndexOrdering = null;

        batchEndingPosition = new ArrayList<>();
    }

    private PagesIndex(
            OrderingCompiler orderingCompiler,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators,
            List<Type> types,
            int expectedPositions,
            boolean eagerCompact,
            int pagesBatchSize,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        this(orderingCompiler, joinCompiler, blockTypeOperators, types, expectedPositions, eagerCompact);
        this.pagesBatchSize = pagesBatchSize;
        pagesIndexOrdering = createPagesIndexComparator(sortChannels, sortOrders);
    }

    public interface Factory
    {
        PagesIndex newPagesIndex(List<Type> types, int expectedPositions);

        PagesIndex newPagesIndex(List<Type> types, int expectedPositions, int pagesBatchSize, List<Integer> sortChannels, List<SortOrder> sortOrders);
    }

    public static class TestingFactory
            implements Factory
    {
        public static final TypeOperators TYPE_OPERATORS = new TypeOperators();
        private static final OrderingCompiler ORDERING_COMPILER = new OrderingCompiler(TYPE_OPERATORS);
        private final JoinCompiler joinCompiler;
        private static final BlockTypeOperators TYPE_OPERATOR_FACTORY = new BlockTypeOperators(TYPE_OPERATORS);
        private final boolean eagerCompact;

        public TestingFactory(boolean eagerCompact)
        {
            this(eagerCompact, true);
        }

        public TestingFactory(boolean eagerCompact, boolean enableSingleChannelBigintLookupSource)
        {
            this.eagerCompact = eagerCompact;
            joinCompiler = new JoinCompiler(TYPE_OPERATORS, enableSingleChannelBigintLookupSource);
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions)
        {
            return new PagesIndex(ORDERING_COMPILER, joinCompiler, TYPE_OPERATOR_FACTORY, types, expectedPositions, eagerCompact);
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions, int pagesBatchSize, List<Integer> sortChannels, List<SortOrder> sortOrders)
        {
            return new PagesIndex(ORDERING_COMPILER, joinCompiler, TYPE_OPERATOR_FACTORY, types, expectedPositions, eagerCompact, pagesBatchSize, sortChannels, sortOrders);
        }
    }

    public static class DefaultFactory
            implements Factory
    {
        private final OrderingCompiler orderingCompiler;
        private final JoinCompiler joinCompiler;
        private final boolean eagerCompact;
        private final BlockTypeOperators blockTypeOperators;

        @Inject
        public DefaultFactory(OrderingCompiler orderingCompiler, JoinCompiler joinCompiler, FeaturesConfig featuresConfig, BlockTypeOperators blockTypeOperators)
        {
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.eagerCompact = featuresConfig.isPagesIndexEagerCompactionEnabled();
            this.blockTypeOperators = requireNonNull(blockTypeOperators, "blockTypeOperators is null");
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions)
        {
            return new PagesIndex(orderingCompiler, joinCompiler, blockTypeOperators, types, expectedPositions, eagerCompact);
        }

        @Override
        public PagesIndex newPagesIndex(List<Type> types, int expectedPositions, int pagesBatchSize, List<Integer> sortChannels, List<SortOrder> sortOrders)
        {
            return new PagesIndex(orderingCompiler, joinCompiler, blockTypeOperators, types, expectedPositions, eagerCompact, pagesBatchSize, sortChannels, sortOrders);
        }
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public void storePage(Page page)
    {
        // ignore empty pages
        if (page.getPositionCount() == 0) {
            return;
        }
        checkArgument(page.getSeqNum().isPresent() && page.getId().isPresent());
        String pageSeqNum = page.getSeqNum().get();
        String pageId = page.getId().isPresent() ? page.getId().get() : "";
        if (!seqNumToPageBySplit.containsKey(pageId)) {
            seqNumToPageBySplit.put(pageId, new HashMap<>());
        }
        Map<String, List<Page>> seqNumToPage = seqNumToPageBySplit.get(pageId);
        if (!seqNumToPage.containsKey(pageSeqNum)) {
            seqNumToPage.put(pageSeqNum, new ArrayList<>());
        }
        seqNumToPage.get(pageSeqNum).add(page);
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public LongArrayList getValueAddresses()
    {
        return valueAddresses;
    }

    public ObjectArrayList<Block> getChannel(int channel)
    {
        return channels[channel];
    }

    public void clear()
    {
        modificationCount++;
        for (ObjectArrayList<Block> channel : channels) {
            channel.clear();
            channel.trim();
        }
        valueAddresses.clear();
        valueAddresses.trim();
        positionCount = 0;
        nextBlockToCompact = 0;
        pagesMemorySize = 0;
        positionCounts.clear();
        positionCounts.trim();
        pageCount = 0;

        estimatedSize = calculateEstimatedSize();
    }

    public void addPage(Page page)
    {
        modificationCount++;
        // ignore empty pages
        if (page.getPositionCount() == 0) {
            return;
        }
        String pageId = page.getId().isPresent() ? page.getId().get() : "";
        if (!pagesBySplit.containsKey(pageId)) {
            pagesBySplit.put(pageId, new ArrayList<>());
        }
        pagesBySplit.get(pageId).add(pageCount);

        pageCount++;
        positionCount += page.getPositionCount();
        positionCounts.add(page.getPositionCount());
        batchEndingPosition.add(positionCount);

        int pageIndex = (channels.length > 0) ? channels[0].size() : 0;
        for (int i = 0; i < channels.length; i++) {
            Block block = page.getBlock(i);
            if (eagerCompact) {
                block = block.copyRegion(0, block.getPositionCount());
            }
            channels[i].add(block);
            pagesMemorySize += block.getRetainedSizeInBytes();
        }

        // this uses a long[] internally, so cap size to a nice round number for safety
        int resultingSize = valueAddresses.size() + page.getPositionCount();
        if (resultingSize < 0 || resultingSize >= 2_000_000_000) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of pages index cannot exceed 2 billion entries");
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            valueAddresses.add(encodeSyntheticAddress(pageIndex, position));
        }
        estimatedSize = calculateEstimatedSize();
    }

    public void addSeqPages()
    {
        Comparator<String> seqNumkeyComparator = (str1, str2) -> {
            List<Integer> str1Decomposed = Arrays.stream(str1.split("_")).map(Integer::parseInt).collect(Collectors.toList());
            List<Integer> str2Decomposed = Arrays.stream(str2.split("_")).map(Integer::parseInt).collect(Collectors.toList());
            int i = 0;
            while (i < str1Decomposed.size() && i < str2Decomposed.size()) {
                if (str1Decomposed.get(i) < str2Decomposed.get(i)) {
                    return -1;
                }
                else if (str1Decomposed.get(i) > str2Decomposed.get(i)) {
                    return 1;
                }
                i++;
            }
            throw new IllegalStateException();
        };
        for (Map<String, List<Page>> seqNumToPage : seqNumToPageBySplit.values()) {
            Set<String> keys = seqNumToPage.keySet();
            List<String> sortedKeys = new ArrayList<>(keys);
            sortedKeys.sort(seqNumkeyComparator);
            for (String sortedKey : sortedKeys) {
                for (Page page : seqNumToPage.get(sortedKey)) {
                    addPage(page);
                }
            }
        }
    }

    public void addAndSortPage(Page page)
    {
        // ignore empty pages
        if (page.getPositionCount() == 0) {
            return;
        }
        pageCount++;
        positionCount += page.getPositionCount();
        pagesBatchIter++;
        boolean batchCollected = false;
        if (pagesBatchIter == pagesBatchSize || ((pagesBatchSize < 0) && page.isSplitFinishedPage())) {
            batchEndingPosition.add(positionCount);
            pagesBatchIter = 0;
            batchCollected = true;
        }

        positionCounts.add(page.getPositionCount());

        int pageIndex = (channels.length > 0) ? channels[0].size() : 0;
        for (int i = 0; i < channels.length; i++) {
            Block block = page.getBlock(i);
            if (eagerCompact) {
                block = block.copyRegion(0, block.getPositionCount());
            }
            channels[i].add(block);
            pagesMemorySize += block.getRetainedSizeInBytes();
        }
        for (int position = 0; position < page.getPositionCount(); position++) {
            long sliceAddress = encodeSyntheticAddress(pageIndex, position);

            // this uses a long[] internally, so cap size to a nice round number for safety
            if (valueAddresses.size() >= 2_000_000_000) {
                throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of pages index cannot exceed 2 billion entries");
            }
            valueAddresses.add(sliceAddress);
        }
        estimatedSize = calculateEstimatedSize();
        if (batchCollected) {
            long time = System.nanoTime();
            pagesIndexOrdering.sort(this, batchEndingPosition.size() >= 2 ? batchEndingPosition.get(batchEndingPosition.size() - 2) : 0, batchEndingPosition.get(batchEndingPosition.size() - 1));
            long elapsed = System.nanoTime() - time;
//            log.debug("Sorting %d entries took %.3f s", batchEndingPosition.get(batchEndingPosition.size() - 1) - (batchEndingPosition.size() >= 2 ? batchEndingPosition.get(batchEndingPosition.size() - 2) : 0), elapsed / 1_000_000_000.0);
        }
    }

    // deliberately coupled it to pagesindex to get best performance
    private static class MinPQ
    {
        private final int[] pq;                    // store items at indices 1 to n
        private int n;                       // number of items on priority queue
        PagesIndexComparator comparator;  // optional comparator
        PagesIndex pagesIndex;

        /**
         * Initializes an empty priority queue with the given initial capacity,
         * using the given comparator.
         *
         * @param initCapacity the initial capacity of this priority queue
         * @param comparator the order in which to compare the keys
         */
        public MinPQ(int initCapacity, PagesIndexComparator comparator, PagesIndex pagesIndex)
        {
            this.comparator = comparator;
            this.pagesIndex = pagesIndex;
            pq = new int[initCapacity + 1];
            for (int i = 0; i <= initCapacity; i++) {
                pq[i] = -1;
            }
            n = 0;
        }

        /**
         * Returns true if this priority queue is empty.
         *
         * @return {@code true} if this priority queue is empty;
         * {@code false} otherwise
         */
        public boolean isEmpty()
        {
            return n == 0;
        }

        /**
         * Returns the number of keys on this priority queue.
         *
         * @return the number of keys on this priority queue
         */
        public int size()
        {
            return n;
        }

        /**
         * Returns a smallest key on this priority queue.
         *
         * @return a smallest key on this priority queue
         */
        public int min()
        {
            return pq[1];
        }

        /**
         * Adds a new key to this priority queue.
         *
         * @param x the key to add to this priority queue
         */
        public void insert(int x)
        {
            // add x, and percolate it up to maintain heap invariant
            pq[++n] = x;
            swim(n);
        }

        /**
         * Removes and returns a smallest key on this priority queue.
         *
         * @return a smallest key on this priority queue
         */
        public int delMin()
        {
            int min = pq[1];
            exch(1, n--);
            sink(1);
            pq[n + 1] = -1;     // to avoid loitering and help with garbage collection
            return min;
        }

        /***************************************************************************
         * Helper functions to restore the heap invariant.
         ***************************************************************************/

        private void swim(int k)
        {
            while (k > 1 && greater(k / 2, k)) {
                exch(k / 2, k);
                k = k / 2;
            }
        }

        private void sink(int k)
        {
            while (2 * k <= n) {
                int j = 2 * k;
                if (j < n && greater(j, j + 1)) {
                    j++;
                }
                if (!greater(k, j)) {
                    break;
                }
                exch(k, j);
                k = j;
            }
        }

        /***************************************************************************
         * Helper functions for compares and swaps.
         ***************************************************************************/
        private boolean greater(int i, int j)
        {
            return comparator.compareTo(pagesIndex, pq[i], pq[j]) > 0;
        }

        private void exch(int i, int j)
        {
            int swap = pq[i];
            pq[i] = pq[j];
            pq[j] = swap;
        }
    }

    public LongArrayList mergePages(Map<String, int[]> sortedGroups)
    {
        int[][] efficientSortedGroups = new int[sortedGroups.size()][];
        int[] positionGroup = new int[positionCount];
        boolean[] endingTable = new boolean[positionCount];
        int[] groupIdx = new int[sortedGroups.size()];
        int idx = 0;
        for (int[] sortedGroup : sortedGroups.values()) {
            efficientSortedGroups[idx] = sortedGroup;
            for (int ar : sortedGroup) {
                positionGroup[ar] = idx;
            }
            endingTable[sortedGroup[sortedGroup.length - 1]] = true;
            idx++;
        }
        PagesIndexComparator mergePagesIndexComparator = pagesIndexOrdering.getComparator();
        MinPQ pq = new MinPQ(sortedGroups.size(), mergePagesIndexComparator, this);
        sortedGroups.forEach((key, value) -> pq.insert(value[0]));
        long[] newValueAddress = new long[positionCount];
        long[] currentValueAddress = valueAddresses.elements();
        idx = 0;
        while (pq.size() > 0) {
            int cur = pq.delMin();
            newValueAddress[idx++] = currentValueAddress[cur];
            int group = positionGroup[cur];
            if (!endingTable[cur]) {
                groupIdx[group] = groupIdx[group] + 1;
                int next = efficientSortedGroups[group][groupIdx[group]];
                pq.insert(next);
            }
        }
        return LongArrayList.wrap(newValueAddress);
    }

    public Map<String, List<Integer>> getPagesBySplit()
    {
        return pagesBySplit;
    }

    public IntArrayList getPositionCounts()
    {
        return positionCounts;
    }

    public void mergePagesIndex(PagesIndex pagesIndex, SortOperator.SortOperatorFactory.Mode mode)
    {
        Map<String, int[]> sortedGroups = new HashMap<>();
        if (mode == SortOperator.SortOperatorFactory.Mode.DYNAMIC) {
            // firstly sort the last batch
            if (this.pageCount > 0) {
                if (batchEndingPosition.isEmpty() || (batchEndingPosition.get(batchEndingPosition.size() - 1) != positionCount)) {
                    batchEndingPosition.add(positionCount);
                    pagesIndexOrdering.sort(this, batchEndingPosition.size() >= 2 ? batchEndingPosition.get(batchEndingPosition.size() - 2) : 0, batchEndingPosition.get(batchEndingPosition.size() - 1));
                }
            }
            List<Integer> batchStartingPositions = new ArrayList<>();

            if (batchEndingPosition.size() >= 2) {
                batchStartingPositions.addAll(batchEndingPosition.subList(0, batchEndingPosition.size() - 1));
            }
            if (batchEndingPosition.size() > 0) {
                batchStartingPositions.add(0, 0);
            }

            for (int i = 0; i < batchStartingPositions.size(); i++) {
                int[] currentGroupIdxs = new int[batchEndingPosition.get(i) - batchStartingPositions.get(i)];
                for (int j = 0; j < currentGroupIdxs.length; j++) {
                    currentGroupIdxs[j] = j + batchStartingPositions.get(i);
                }
                sortedGroups.put("up" + i, currentGroupIdxs);
            }

            AtomicInteger ai = new AtomicInteger();
            List<Integer> pagesStartingPositions = pagesIndex.getPositionCounts().stream().map(ai::addAndGet).collect(Collectors.toList());
            pagesStartingPositions.add(0, 0);

            for (Map.Entry<String, List<Integer>> pagesGroupIdxs : pagesIndex.getPagesBySplit().entrySet()) {
                int[] currentGroupIdxs = pagesGroupIdxs.getValue().stream().flatMapToInt(i -> IntStream.range(pagesStartingPositions.get(i), pagesStartingPositions.get(i + 1)).map(j -> j + this.positionCount)).toArray();
                sortedGroups.put(pagesGroupIdxs.getKey(), currentGroupIdxs);
            }
        }
        else {
            AtomicInteger ai = new AtomicInteger();
            List<Integer> pagesStartingPositions = pagesIndex.getPositionCounts().stream().map(ai::addAndGet).collect(Collectors.toList());
            pagesStartingPositions.add(0, 0);
            for (Map.Entry<String, List<Integer>> pagesGroupIdxs : pagesIndex.getPagesBySplit().entrySet()) {
                int[] currentGroupIdxs = pagesGroupIdxs.getValue().stream().flatMapToInt(i -> IntStream.range(pagesStartingPositions.get(i), pagesStartingPositions.get(i + 1)).map(j -> j + this.positionCount)).toArray();
                sortedGroups.put(pagesGroupIdxs.getKey(), currentGroupIdxs);
            }
        }

        List<Page> pages = JoinUtils.channelsToPages(ImmutableList.copyOf(pagesIndex.channels));
        for (Page page : pages) {
            this.addPage(page);
        }
        if (!(mode == SortOperator.SortOperatorFactory.Mode.STATIC && sortedGroups.size() == 1)) {
            long timeNow = System.nanoTime();
            LongArrayList newValueAddress = this.mergePages(sortedGroups);
            log.debug("Merge %d sorting groups of size %d on %d take %.3f s", sortedGroups.size(), this.getPositionCount(), this.hashCode(), (System.nanoTime() - timeNow) / 1_000_000_000.0);
            valueAddresses.removeElements(valueAddresses.size() - newValueAddress.size(), valueAddresses.size());
            valueAddresses.addAll(newValueAddress);
        }
    }

    public DataSize getEstimatedSize()
    {
        return DataSize.ofBytes(estimatedSize);
    }

    public void compact()
    {
        modificationCount++;
        if (eagerCompact || channels.length == 0) {
            return;
        }
        for (int channel = 0; channel < types.size(); channel++) {
            ObjectArrayList<Block> blocks = channels[channel];
            for (int i = nextBlockToCompact; i < blocks.size(); i++) {
                Block block = blocks.get(i);

                // Copy the block to compact its size
                Block compactedBlock = block.copyRegion(0, block.getPositionCount());
                blocks.set(i, compactedBlock);
                pagesMemorySize -= block.getRetainedSizeInBytes();
                pagesMemorySize += compactedBlock.getRetainedSizeInBytes();
            }
        }
        nextBlockToCompact = channels[0].size();
        estimatedSize = calculateEstimatedSize();
    }

    private long calculateEstimatedSize()
    {
        long elementsSize = (channels.length > 0) ? sizeOf(channels[0].elements()) : 0;
        long channelsArraySize = elementsSize * channels.length;
        long addressesArraySize = sizeOf(valueAddresses.elements());
        long positionCountsSize = sizeOf(positionCounts.elements());
        return INSTANCE_SIZE + pagesMemorySize + channelsArraySize + addressesArraySize + positionCountsSize;
    }

    public Type getType(int channel)
    {
        return types.get(channel);
    }

    @Override
    public void swap(int a, int b)
    {
        // Not changing modificationCount. This is part of sorting and we change modificationCount for sorting only once.
        // TODO remove the method from PagesIndex interface
        long[] elements = valueAddresses.elements();
        long temp = elements[a];
        elements[a] = elements[b];
        elements[b] = temp;
    }

    private int buildPage(int position, int endPosition, PageBuilder pageBuilder)
    {
        while (!pageBuilder.isFull() && position < endPosition) {
            long pageAddress = valueAddresses.getLong(position);
            int blockIndex = decodeSliceIndex(pageAddress);
            int blockPosition = decodePosition(pageAddress);

            // append the row
            pageBuilder.declarePosition();
            for (int channel = 0; channel < channels.length; channel++) {
                Block block = channels[channel].get(blockIndex);
                pageBuilder.getBlockBuilder(channel).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(blockPosition));
            }

            position++;
        }

        return position;
    }

    public void appendTo(int channel, int position, BlockBuilder output)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        output.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(blockPosition));
    }

    public boolean isNull(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return block.isNull(blockPosition);
    }

    public boolean getBoolean(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getBoolean(block, blockPosition);
    }

    public long getLong(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getLong(block, blockPosition);
    }

    public double getDouble(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getDouble(block, blockPosition);
    }

    public Slice getSlice(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getSlice(block, blockPosition);
    }

    public Object getObject(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return types.get(channel).getObject(block, blockPosition);
    }

    public Block getSingleValueBlock(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);

        Block block = channels[channel].get(decodeSliceIndex(pageAddress));
        int blockPosition = decodePosition(pageAddress);
        return block.getSingleValueBlock(blockPosition);
    }

    public Block getRawBlock(int channel, int position)
    {
        long pageAddress = valueAddresses.getLong(position);
        return channels[channel].get(decodeSliceIndex(pageAddress));
    }

    public int getRawBlockPosition(int position)
    {
        long pageAddress = valueAddresses.getLong(position);
        return decodePosition(pageAddress);
    }

    public void sort(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        sort(createPagesIndexComparator(sortChannels, sortOrders), 0, getPositionCount());
    }

    public void sort(PagesIndexOrdering pagesIndexOrdering)
    {
        sort(pagesIndexOrdering, 0, getPositionCount());
    }

    public void sort(PagesIndexOrdering pagesIndexOrdering, int startPosition, int endPosition)
    {
        requireNonNull(pagesIndexOrdering, "pagesIndexOrdering is null");
        modificationCount++;
        pagesIndexOrdering.sort(this, startPosition, endPosition);
    }

    public boolean positionIdenticalToPosition(PagesHashStrategy partitionHashStrategy, int leftPosition, int rightPosition)
    {
        long leftAddress = valueAddresses.getLong(leftPosition);
        int leftPageIndex = decodeSliceIndex(leftAddress);
        int leftPagePosition = decodePosition(leftAddress);

        long rightAddress = valueAddresses.getLong(rightPosition);
        int rightPageIndex = decodeSliceIndex(rightAddress);
        int rightPagePosition = decodePosition(rightAddress);

        return partitionHashStrategy.positionIdenticalToPosition(leftPageIndex, leftPagePosition, rightPageIndex, rightPagePosition);
    }

    public boolean positionIdenticalToRow(PagesHashStrategy pagesHashStrategy, int indexPosition, int rightPosition, Page rightPage)
    {
        long pageAddress = valueAddresses.getLong(indexPosition);
        int pageIndex = decodeSliceIndex(pageAddress);
        int pagePosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionIdenticalToRow(pageIndex, pagePosition, rightPosition, rightPage);
    }

    public PagesIndexOrdering createPagesIndexComparator(List<Integer> sortChannels, List<SortOrder> sortOrders)
    {
        List<Type> sortTypes = sortChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
        return orderingCompiler.compilePagesIndexOrdering(sortTypes, sortChannels, sortOrders);
    }

    public Supplier<LookupSource> createLookupSourceSupplier(Session session, List<Integer> joinChannels)
    {
        return createLookupSourceSupplier(session, joinChannels, Optional.empty(), Optional.empty(), ImmutableList.of());
    }

    public PagesHashStrategy createPagesHashStrategy(List<Integer> joinChannels)
    {
        return createPagesHashStrategy(joinChannels, Optional.empty());
    }

    private PagesHashStrategy createPagesHashStrategy(List<Integer> joinChannels, Optional<List<Integer>> outputChannels)
    {
        try {
            return joinCompiler.compilePagesHashStrategyFactory(types, joinChannels, outputChannels)
                    .createPagesHashStrategy(ImmutableList.copyOf(channels));
        }
        catch (Exception e) {
            log.error(e, "Lookup source compile failed for types=%s error=%s", types, e);
        }

        // if compilation fails, use interpreter
        return new SimplePagesHashStrategy(
                types,
                outputChannels.orElseGet(() -> rangeList(types.size())),
                ImmutableList.copyOf(channels),
                joinChannels,
                Optional.empty(),
                blockTypeOperators);
    }

    public PagesIndexComparator createChannelComparator(int leftChannel, int rightChannel)
    {
        checkArgument(types.get(leftChannel).equals(types.get(rightChannel)), "comparing channels of different types: %s and %s", types.get(leftChannel), types.get(rightChannel));
        return new SimpleChannelComparator(leftChannel, rightChannel, blockTypeOperators.getComparisonUnorderedLastOperator(types.get(leftChannel)));
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories)
    {
        return createLookupSourceSupplier(session, joinChannels, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.empty(), defaultHashArraySizeSupplier());
    }

    public PagesSpatialIndexSupplier createPagesSpatialIndex(
            Session session,
            int geometryChannel,
            Optional<Integer> radiusChannel,
            OptionalDouble constantRadius,
            Optional<Integer> partitionChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            List<Integer> outputChannels,
            Map<Integer, Rectangle> partitions)
    {
        // TODO probably shouldn't copy to reduce memory and for memory accounting's sake
        List<ObjectArrayList<Block>> channels = ImmutableList.copyOf(this.channels);
        return new PagesSpatialIndexSupplier(session, valueAddresses, outputChannels, channels, geometryChannel, radiusChannel, constantRadius, partitionChannel, spatialRelationshipTest, filterFunctionFactory, partitions);
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            Optional<List<Integer>> outputChannels,
            HashArraySizeSupplier hashArraySizeSupplier)
    {
        List<ObjectArrayList<Block>> channels = ImmutableList.copyOf(this.channels);
        LookupSourceSupplierFactory lookupSourceFactory = joinCompiler.compileLookupSourceFactory(types, joinChannels, sortChannel, outputChannels);
        return lookupSourceFactory.createLookupSourceSupplier(
                session,
                valueAddresses,
                channels,
                filterFunctionFactory,
                sortChannel,
                searchFunctionFactories,
                hashArraySizeSupplier);
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positionCount", positionCount)
                .add("types", types)
                .add("estimatedSize", estimatedSize)
                .toString();
    }

    public Iterator<Page> getPages()
    {
        return new AbstractIterator<>()
        {
            private final int startingModificationCount = modificationCount;
            private int currentPage;

            @Override
            protected Page computeNext()
            {
                if (currentPage == pageCount) {
                    if (startingModificationCount != modificationCount) {
                        throw new ConcurrentModificationException("PagesIndex mutated during iteration: %s != %s".formatted(startingModificationCount, modificationCount));
                    }
                    return endOfData();
                }

                int positions = positionCounts.getInt(currentPage);
                Block[] blocks = Stream.of(channels)
                        .map(channel -> channel.get(currentPage))
                        .toArray(Block[]::new);

                currentPage++;
                return new Page(positions, blocks);
            }
        };
    }

    public Iterator<Page> getSortedPages()
    {
        return getSortedPagesFromRange(0, positionCount);
    }

    /**
     * Get sorted pages from the specified section of the PagesIndex.
     *
     * @param start start position of the section, inclusive
     * @param end end position of the section, exclusive
     * @return iterator of pages
     */
    public Iterator<Page> getSortedPages(int start, int end)
    {
        checkArgument(start >= 0 && end <= positionCount, "position range out of bounds");
        checkArgument(start <= end, "invalid position range");
        return getSortedPagesFromRange(start, end);
    }

    private Iterator<Page> getSortedPagesFromRange(int start, int end)
    {
        return new AbstractIterator<>()
        {
            private final int startingModificationCount = modificationCount;
            private int currentPosition = start;
            private final PageBuilder pageBuilder = new PageBuilder(types);

            @Override
            public Page computeNext()
            {
                currentPosition = buildPage(currentPosition, end, pageBuilder);
                if (pageBuilder.isEmpty()) {
                    if (startingModificationCount != modificationCount) {
                        throw new ConcurrentModificationException("PagesIndex mutated during iteration: %s != %s".formatted(startingModificationCount, modificationCount));
                    }
                    return endOfData();
                }
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return page;
            }
        };
    }

    public long getEstimatedMemoryRequiredToCreateLookupSource(
            HashArraySizeSupplier hashArraySizeSupplier,
            Optional<Integer> sortChannel,
            List<Integer> joinChannels)
    {
        // channels and valueAddresses are shared between PagesIndex and JoinHashSupplier and are accounted as part of lookupSourceEstimatedRetainedSizeInBytes
        long lookupSourceEstimatedRetainedSizeInBytes = JoinHashSupplier.getEstimatedRetainedSizeInBytes(
                positionCount,
                valueAddresses,
                ImmutableList.copyOf(channels),
                pagesMemorySize,
                sortChannel,
                getSingleBigintJoinChannel(joinChannels, types),
                hashArraySizeSupplier);
        // PageIndex is retained during LookupSource creation, hence any extra memory retained by the PagesIndex must be accounted here
        long pagesIndexAdditionalRetainedSizeInBytes = getExtraPagesIndexMemoryWithLookupSourceBuild();
        return pagesIndexAdditionalRetainedSizeInBytes + lookupSourceEstimatedRetainedSizeInBytes;
    }

    public long getExtraPagesIndexMemoryWithLookupSourceBuild()
    {
        return INSTANCE_SIZE + sizeOf(positionCounts.elements());
    }
}
