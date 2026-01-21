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
import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SortMergeOuterJoinResultProcessingOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final boolean isInnerJoin;
    private final List<Integer> leftPrimaryKeyChannels;
    private final List<Integer> rightJoinChannels;
    private final List<Integer> outputChannels;
    private final Integer leftColumnsSize;
    private Page currentPrimaryKeyPage;
    private int currentPrimaryKeyRow;
    private final List<BlockTypeOperators.BlockPositionEqual> equalOperators;
    private final List<Page> leftJoinResult;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final PagesIndex pagesIndex;
    private final AtomicInteger sortFinishedCnt;
    private final SettableFuture<Void> sortFinishedFuture;
    private final boolean pkAndJoinColumnsEqual;
    private boolean isFinished;

    public SortMergeOuterJoinResultProcessingOperator(
            OperatorContext operatorContext,
            boolean isInnerJoin,
            List<Integer> leftPrimaryKeyChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels,
            List<Integer> outputChannels,
            Integer leftColumnsSize,
            List<BlockTypeOperators.BlockPositionEqual> equalOperators,
            List<Page> leftJoinResult,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            PagesIndex pagesIndex,
            AtomicInteger sortFinishedCnt,
            SettableFuture<Void> sortFinishedFuture)
    {
        this.operatorContext = operatorContext;
        this.isInnerJoin = isInnerJoin;
        this.leftPrimaryKeyChannels = requireNonNull(leftPrimaryKeyChannels);
        this.rightJoinChannels = requireNonNull(rightJoinChannels);
        this.outputChannels = requireNonNull(outputChannels);
        this.leftColumnsSize = requireNonNull(leftColumnsSize);
        this.equalOperators = equalOperators;
        this.leftJoinResult = leftJoinResult;
        this.sortChannels = sortChannels;
        this.sortOrder = sortOrder;
        this.pagesIndex = pagesIndex;
        this.sortFinishedCnt = sortFinishedCnt;
        this.sortFinishedFuture = sortFinishedFuture;
        this.currentPrimaryKeyPage = null;
        this.pkAndJoinColumnsEqual = leftPrimaryKeyChannels.stream().sorted().collect(Collectors.toList())
                .equals(leftJoinChannels.stream().sorted().collect(Collectors.toList()));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return true;
    }

    @Override
    public void addInput(Page page)
    {
        Page[] extractedPages = extractPages(page, isInnerJoin);
        pagesIndex.addPage(extractedPages[0]);
        leftJoinResult.add(extractedPages[1]);
    }

    // specification: left: probeSide, right: outerSide
    // input format: probeSideData, outerSideData
    private Page[] extractPages(Page page, boolean isInnerJoin)
    {
        int[] leftRetainedPositions = new int[page.getPositionCount()];
        int[] outputRetainedPositions = new int[page.getPositionCount()];
        int leftRetainedPositionsPtr = 0;
        int outputRetainedPositionsPtr = 0;
        for (int i = 0; i < page.getPositionCount(); i++) {
            boolean rightNull = page.getBlock(rightJoinChannels.get(0)).isNull(i);
            if (rightNull) {
                leftRetainedPositions[leftRetainedPositionsPtr] = i;
                leftRetainedPositionsPtr += 1;
            }
            else {
                if (!primaryKeyColumnsEqual(page, i)) {
                    leftRetainedPositions[leftRetainedPositionsPtr] = i;
                    leftRetainedPositionsPtr += 1;
                }
                outputRetainedPositions[outputRetainedPositionsPtr] = i;
                outputRetainedPositionsPtr += 1;
            }
        }

        int[] leftColumnIdxs = new int[leftColumnsSize + 1];
        for (int i = 0; i < leftColumnsSize; i++) {
            leftColumnIdxs[i] = i;
        }

        int[] outputColumnIdxs = new int[outputChannels.size()];
        for (int i = 0; i < outputChannels.size(); i++) {
            outputColumnIdxs[i] = outputChannels.get(i);
        }
        leftColumnIdxs[leftColumnsSize] = page.getChannelCount() - 1;
        Page leftPage = page.copyPositions(leftRetainedPositions, 0, leftRetainedPositionsPtr)
                .getColumns(leftColumnIdxs);
        Page outputPage = page.copyPositions(outputRetainedPositions, 0, outputRetainedPositionsPtr)
                .getColumns(outputColumnIdxs);
        return new Page[] {leftPage, outputPage};
    }

    private Boolean primaryKeyColumnsEqual(Page page, int row)
    {
        if (currentPrimaryKeyPage == null) {
            currentPrimaryKeyPage = page;
            currentPrimaryKeyRow = row;
            return false;
        }
        for (int i = 0; i < leftPrimaryKeyChannels.size(); i++) {
            BlockTypeOperators.BlockPositionEqual equalOperator = equalOperators.get(i);
            Block comingBlock = page.getBlock(leftPrimaryKeyChannels.get(i));
            Block currentBlock = currentPrimaryKeyPage.getBlock(leftPrimaryKeyChannels.get(i));
            if (!equalOperator.equal(currentBlock, currentPrimaryKeyRow, comingBlock, row)) {
                currentPrimaryKeyPage = page;
                currentPrimaryKeyRow = row;
                return false;
            }
        }
        return true;
    }

    @Override
    public Page getOutput()
    {
        throw new UnsupportedOperationException("Should not get output from SortOperator");
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

    @Override
    public void finish()
    {
        if (!pkAndJoinColumnsEqual) {
            pagesIndex.sort(sortChannels, sortOrder);
        }
        if (sortFinishedCnt.incrementAndGet() == 2) {
            sortFinishedFuture.set(null);
        }
        isFinished = true;
    }

    public static class SortMergeOuterJoinResultProcessingOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final SortMergeJoinBridge joinBridge;
        private final boolean isInnerJoin;
        private final List<Integer> leftPrimaryKeyChannels;
        private final List<Integer> leftJoinChannels;
        private final List<Integer> rightJoinChannels;
        private final List<Integer> outputChannels;
        private final Integer leftColumnsSize;
        private final List<BlockTypeOperators.BlockPositionEqual> equalOperators;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private boolean closed;

        public SortMergeOuterJoinResultProcessingOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                SortMergeJoinBridge joinBridge,
                boolean isInnerJoin,
                List<Integer> leftPrimaryKeyChannels,
                List<Integer> leftJoinChannels,
                List<Integer> rightJoinChannels,
                List<Integer> outputChannels,
                Integer leftColumnsSize,
                List<BlockTypeOperators.BlockPositionEqual> equalOperators,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridge = requireNonNull(joinBridge, "lookupSourceFactoryManager is null");
            this.isInnerJoin = isInnerJoin;
            this.leftPrimaryKeyChannels = requireNonNull(leftPrimaryKeyChannels);
            this.leftJoinChannels = requireNonNull(leftJoinChannels);
            this.rightJoinChannels = requireNonNull(rightJoinChannels);
            this.outputChannels = requireNonNull(outputChannels);
            this.leftColumnsSize = requireNonNull(leftColumnsSize);
            this.equalOperators = requireNonNull(equalOperators);
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        }

        @Override
        public SortMergeOuterJoinResultProcessingOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId,
                    SortMergeOuterJoinResultProcessingOperator.class.getSimpleName());
            Integer localPartitioningIndex = driverContext.getLocalPartitioningIndex();
            return new SortMergeOuterJoinResultProcessingOperator(
                    operatorContext,
                    isInnerJoin,
                    leftPrimaryKeyChannels,
                    leftJoinChannels,
                    rightJoinChannels,
                    outputChannels,
                    leftColumnsSize,
                    equalOperators,
                    joinBridge.getLeftJoinResult(localPartitioningIndex),
                    sortChannels,
                    sortOrder,
                    joinBridge.getLeftUpPagesIndex(localPartitioningIndex),
                    joinBridge.getSortFinishedCnt(localPartitioningIndex),
                    joinBridge.getSortFinishedFuture(localPartitioningIndex));
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Parallel hash build cannot be duplicated");
        }
    }
}
