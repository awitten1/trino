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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.OrderingCompiler;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.util.MergeSortedPages.mergeSortedPages;
import static java.util.Objects.requireNonNull;

public class SortOperator
        implements Operator
{
    public static class SortOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final boolean spillEnabled;
        private final Optional<SpillerFactory> spillerFactory;
        private final OrderingCompiler orderingCompiler;
        private final SortMergeJoinBridge bridge;
        private final Placement placement;
        private final int finishedCnt;
        private final Mode mode;

        public enum Placement
        {
            LEFT_UP,
            LEFT_DOWN,
            RIGHT_UP,
            RIGHT_DOWN
        }

        public enum Mode
        {
            STATIC,
            DYNAMIC
        }

        private boolean closed;

        public SortOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                boolean spillEnabled,
                Optional<SpillerFactory> spillerFactory,
                OrderingCompiler orderingCompiler,
                SortMergeJoinBridge bridge,
                Placement placement,
                int finishedCnt,
                Mode mode)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));

            this.spillEnabled = spillEnabled;
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.bridge = bridge;
            this.placement = placement;
            this.finishedCnt = finishedCnt;
            this.mode = mode;
            checkArgument(!spillEnabled || spillerFactory.isPresent(), "Spiller Factory is not present when spill is enabled");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            Integer localPartitioningIndex = driverContext.getLocalPartitioningIndex();
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, SortOperator.class.getSimpleName());
            PagesIndex pagesIndex;
            ExecutionType executionType;
            if (placement == Placement.LEFT_UP) {
                pagesIndex = bridge.getLeftUpPagesIndex(localPartitioningIndex);
            }
            else if (placement == Placement.LEFT_DOWN) {
                pagesIndex = bridge.getLeftDownPagesIndex(localPartitioningIndex);
            }
            else if (placement == Placement.RIGHT_UP) {
                pagesIndex = bridge.getRightUpPagesIndex(localPartitioningIndex);
            }
            else if (placement == Placement.RIGHT_DOWN) {
                pagesIndex = bridge.getRightDownPagesIndex(localPartitioningIndex);
            }
            else {
                throw new UnsupportedOperationException("unsupported placement");
            }

            if (placement == Placement.LEFT_UP || placement == Placement.RIGHT_UP) {
                if (mode == Mode.STATIC) {
                    executionType = ExecutionType.STORE_SORT;
                }
                else {
                    executionType = ExecutionType.SORT_MERGE;
                }
            }
            else {
                if (mode == Mode.STATIC) {
                    executionType = ExecutionType.STORE;
                }
                else {
                    executionType = ExecutionType.STORE_MERGE;
                }
            }

            return new SortOperator(
                    operatorContext,
                    sourceTypes,
                    sortChannels,
                    sortOrder,
                    pagesIndex,
                    spillEnabled,
                    spillerFactory,
                    orderingCompiler,
                    bridge.getSortFinishedCnt(localPartitioningIndex),
                    finishedCnt,
                    executionType,
                    bridge.getSortFinishedFuture(localPartitioningIndex));
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("duplicate not supported by sort operator");
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    public enum ExecutionType
    {
        SORT_MERGE,
        STORE_MERGE,
        STORE_SORT,
        STORE
    }

    private final OperatorContext operatorContext;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final LocalMemoryContext revocableMemoryContext;
    private final LocalMemoryContext localUserMemoryContext;

    private final PagesIndex pageIndex;

    private final List<Type> sourceTypes;

    private final boolean spillEnabled;
    private final Optional<SpillerFactory> spillerFactory;
    private final OrderingCompiler orderingCompiler;

    private Optional<Spiller> spiller = Optional.empty();
    private ListenableFuture<DataSize> spillInProgress = immediateFuture(DataSize.ofBytes(0));
    private Runnable finishMemoryRevoke = () -> {};
    private final SettableFuture<Void> sortFinishedFuture;
    private Iterator<Optional<Page>> sortedPages;
    private final AtomicInteger sortFinishedCnt;
    private final int finishedCnt;
    private final ExecutionType executionType;

    private State state = State.NEEDS_INPUT;

    private static final Logger log = Logger.get(SortOperator.class);

    public SortOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            PagesIndex pageIndex,
            boolean spillEnabled,
            Optional<SpillerFactory> spillerFactory,
            OrderingCompiler orderingCompiler,
            AtomicInteger sortFinishedCnt,
            int finishedCnt,
            ExecutionType executionType,
            SettableFuture<Void> sortFinishedFuture)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.revocableMemoryContext = operatorContext.localRevocableMemoryContext();

        this.pageIndex = pageIndex;
        this.spillEnabled = spillEnabled;
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        this.sortFinishedCnt = sortFinishedCnt;
        this.finishedCnt = finishedCnt;
        this.executionType = executionType;
        this.sortFinishedFuture = sortFinishedFuture;
        checkArgument(!spillEnabled || spillerFactory.isPresent(), "Spiller Factory is not present when spill is enabled");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (!spillInProgress.isDone()) {
            return;
        }
        checkSuccess(spillInProgress, "spilling failed");

        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;

            // Convert revocable memory to user memory as sortedPages holds on to memory so we no longer can revoke.
            if (revocableMemoryContext.getBytes() > 0) {
                long currentRevocableBytes = revocableMemoryContext.getBytes();
                revocableMemoryContext.setBytes(0);
                if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + currentRevocableBytes)) {
                    // TODO: this might fail (even though we have just released memory), but we
                    // don't
                    // have a proper way to atomically convert memory reservations
                    revocableMemoryContext.setBytes(currentRevocableBytes);
                    // spill since revocable memory could not be converted to user memory
                    // immediately
                    // TODO: this should be asynchronous
                    getFutureValue(spillToDisk());
                    finishMemoryRevoke.run();
                }
            }
            if (executionType == ExecutionType.STORE || executionType == ExecutionType.STORE_MERGE) {
                pageIndex.addSeqPages();
            }
            if (executionType == ExecutionType.STORE_SORT) {
                pageIndex.sort(sortChannels, sortOrder);
            }
            Iterator<Page> sortedPagesIndex = pageIndex.getSortedPages();

            List<WorkProcessor<Page>> spilledPages = getSpilledPages();
            if (spilledPages.isEmpty()) {
                sortedPages = transform(sortedPagesIndex, Optional::of);
            }
            else {
                sortedPages = mergeSpilledAndMemoryPages(spilledPages, sortedPagesIndex).yieldingIterator();
            }
            if (sortFinishedCnt.incrementAndGet() == finishedCnt) {
                sortFinishedFuture.set(null);
                log.debug("sort finished");
            }
            state = State.FINISHED;
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkSuccess(spillInProgress, "spilling failed");

        // TODO: remove when retained memory accounting for pages does not
        // count shared data structures multiple times
        page.compact();
        if ((executionType == ExecutionType.STORE_SORT)) {
            pageIndex.addPage(page);
        }
        else if ((executionType == ExecutionType.STORE_MERGE) || (executionType == ExecutionType.STORE)) {
            pageIndex.storePage(page);
        }
        else if (executionType == ExecutionType.SORT_MERGE) {
            pageIndex.addAndSortPage(page);
        }
        else {
            throw new UnsupportedOperationException();
        }
        updateMemoryUsage();
        if (page.isSplitFinishedPage()) {
            operatorContext.recordSplitFinishedPageProcessed(page);
        }
    }

    @Override
    public Page getOutput()
    {
        throw new UnsupportedOperationException("Should not get output from SortOperator");
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        verify(state == State.NEEDS_INPUT || revocableMemoryContext.getBytes() == 0, "Cannot spill in state: %s", state);
        return spillToDisk();
    }

    private ListenableFuture<Void> spillToDisk()
    {
        checkSuccess(spillInProgress, "spilling failed");

        if (revocableMemoryContext.getBytes() == 0) {
            verify(pageIndex.getPositionCount() == 0 || state == State.HAS_OUTPUT);
            finishMemoryRevoke = () -> {};
            return immediateFuture(null);
        }

        // TODO try pageIndex.compact(); before spilling, as in
        // HashBuilderOperator.startMemoryRevoke()

        if (spiller.isEmpty()) {
            spiller = Optional.of(spillerFactory.get().create(
                    sourceTypes,
                    operatorContext.getSpillContext(),
                    operatorContext.newAggregateSystemMemoryContext()));
        }

        pageIndex.sort(sortChannels, sortOrder);
        spillInProgress = spiller.get().spill(pageIndex.getSortedPages());
        finishMemoryRevoke = () -> {
            pageIndex.clear();
            updateMemoryUsage();
        };

        return asVoid(spillInProgress);
    }

    @Override
    public void finishMemoryRevoke()
    {
        finishMemoryRevoke.run();
        finishMemoryRevoke = () -> {};
    }

    private List<WorkProcessor<Page>> getSpilledPages()
    {
        if (spiller.isEmpty()) {
            return ImmutableList.of();
        }

        return spiller.get().getSpills().stream()
                .map(WorkProcessor::fromIterator)
                .collect(toImmutableList());
    }

    private WorkProcessor<Page> mergeSpilledAndMemoryPages(
            List<WorkProcessor<Page>> spilledPages,
            Iterator<Page> sortedPagesIndex)
    {
        List<WorkProcessor<Page>> sortedStreams = ImmutableList.<WorkProcessor<Page>>builder()
                .addAll(spilledPages)
                .add(WorkProcessor.fromIterator(sortedPagesIndex))
                .build();

        return mergeSortedPages(
                sortedStreams,
                orderingCompiler.compilePageWithPositionComparator(sourceTypes, sortChannels, sortOrder),
                sourceTypes,
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.getDriverContext().getYieldSignal());
    }

    private void updateMemoryUsage()
    {
        if (spillEnabled && state == State.NEEDS_INPUT) {
            if (pageIndex.getPositionCount() == 0) {
                localUserMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
                revocableMemoryContext.setBytes(0L);
            }
            else {
                localUserMemoryContext.setBytes(0);
                revocableMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
            }
        }
        else {
            revocableMemoryContext.setBytes(0);
            if (!localUserMemoryContext.trySetBytes(pageIndex.getEstimatedSize().toBytes())) {
                pageIndex.compact();
                localUserMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
            }
        }
    }

    @Override
    public void close()
    {
        // pageIndex.clear();
        spiller.ifPresent(Spiller::close);
    }
}
