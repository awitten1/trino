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
import io.trino.operator.DriverContext;
import io.trino.operator.Operator;
import io.trino.operator.OperatorContext;
import io.trino.operator.OperatorFactory;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexComparator;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static java.util.Objects.requireNonNull;

public class PagesMergeOperator
        implements Operator
{
    public static class PagesMergeOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> leftTypes;
        private final List<Type> rightTypes;
        private final List<Integer> leftMergeChannels;
        private final List<Integer> rightMergeChannels;
        private final List<Integer> leftOutputChannels;
        private final List<Integer> rightOutputChannels;
        private final List<BlockTypeOperators.BlockPositionComparison> joinEqualOperators;
        private final SortMergeJoinBridge bridge;
        private boolean closed;

        public PagesMergeOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> leftTypes,
                List<Type> rightTypes,
                List<Integer> leftMergeChannels,
                List<Integer> rightMergeChannels,
                List<Integer> leftOutputChannels,
                List<Integer> rightOutputChannels,
                List<BlockTypeOperators.BlockPositionComparison> joinEqualOperators,
                SortMergeJoinBridge bridge)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.leftTypes = requireNonNull(leftTypes);
            this.rightTypes = requireNonNull(rightTypes);
            this.leftMergeChannels = requireNonNull(leftMergeChannels);
            this.rightMergeChannels = requireNonNull(rightMergeChannels);
            this.leftOutputChannels = requireNonNull(leftOutputChannels);
            this.rightOutputChannels = requireNonNull(rightOutputChannels);
            this.joinEqualOperators = requireNonNull(joinEqualOperators);
            this.bridge = requireNonNull(bridge);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PagesMergeOperator.class.getSimpleName());
            ImmutableList.Builder<SortOrder> sortOrder = ImmutableList.builder();

            for (int i = 0; i < leftMergeChannels.size(); i++) {
                sortOrder.add(ASC_NULLS_LAST);
            }

            List<PagesIndex> pagesIndexPair = bridge.getNextUpSortedPagesPair();
            PagesIndex leftPagesIndex = pagesIndexPair.get(0);
            PagesIndex rightPagesIndex = pagesIndexPair.get(1);
            PagesIndexComparator leftPagesIndexComparator = leftPagesIndex
                    .createPagesIndexComparator(leftMergeChannels, sortOrder.build()).getComparator();
            PagesIndexComparator rightPagesIndexComparator = rightPagesIndex
                    .createPagesIndexComparator(rightMergeChannels, sortOrder.build()).getComparator();

            List<Type> leftOutputTypes = leftOutputChannels.stream().map(leftTypes::get).collect(toImmutableList());
            List<Type> rightOutputTypes = rightOutputChannels.stream().map(rightTypes::get).collect(toImmutableList());

            SortMergePageBuilder pageBuilder = new SortMergePageBuilder(
                    leftPagesIndex,
                    rightPagesIndex,
                    leftOutputTypes,
                    rightOutputTypes,
                    leftOutputChannels,
                    rightOutputChannels);
            return new PagesMergeOperator(
                    operatorContext,
                    leftMergeChannels,
                    rightMergeChannels,
                    leftPagesIndex,
                    rightPagesIndex,
                    leftPagesIndexComparator,
                    rightPagesIndexComparator,
                    bridge.getNextFinishedFuture(),
                    joinEqualOperators,
                    pageBuilder);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories cannot be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final SettableFuture<Void> sortFinishedFuture;
    private final PagesIndex leftSortedPagesIndex;
    private final PagesIndex rightSortedPagesIndex;
    private final List<Integer> leftMergeChannels;
    private final List<Integer> rightMergeChannels;
    private final PagesIndexComparator leftPagesIndexComparator;
    private final PagesIndexComparator rightPagesIndexComparator;
    private final List<BlockTypeOperators.BlockPositionComparison> joinEqualOperators;
    private int leftPos;
    private int rightPos;
    private int previousLeftPos;
    private int previousRightPos;
    private int middleRightPos;
    private final SortMergePageBuilder pageBuilder;
    private boolean finished;

    public PagesMergeOperator(
            OperatorContext operatorContext,
            List<Integer> leftMergeChannels,
            List<Integer> rightMergeChannels,
            PagesIndex leftSortedPagesIndex,
            PagesIndex rightSortedPagesIndex,
            PagesIndexComparator leftPagesIndexComparator,
            PagesIndexComparator rightPagesIndexComparator,
            SettableFuture<Void> sortFinishedFuture,
            List<BlockTypeOperators.BlockPositionComparison> joinEqualOperators,
            SortMergePageBuilder pageBuilder)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.leftSortedPagesIndex = requireNonNull(leftSortedPagesIndex);
        this.rightSortedPagesIndex = requireNonNull(rightSortedPagesIndex);
        this.leftMergeChannels = requireNonNull(leftMergeChannels);
        this.rightMergeChannels = requireNonNull(rightMergeChannels);
        this.leftPagesIndexComparator = requireNonNull(leftPagesIndexComparator);
        this.rightPagesIndexComparator = requireNonNull(rightPagesIndexComparator);
        this.sortFinishedFuture = requireNonNull(sortFinishedFuture);
        this.joinEqualOperators = requireNonNull(joinEqualOperators);
        this.pageBuilder = requireNonNull(pageBuilder);
        this.previousLeftPos = 0;
        this.previousRightPos = 0;
        this.leftPos = 0;
        this.rightPos = 0;
        this.finished = false;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
    }

    @Override
    public boolean isFinished()
    {
        return sortFinishedFuture.isDone() && finished;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return sortFinishedFuture;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        // build the remaining pages
        for (int i = previousLeftPos; i < leftPos; i++) {
            for (int j = (i == previousLeftPos) ? middleRightPos : previousRightPos; j < rightPos; j++) {
                if (!pageBuilder.isFull()) {
                    pageBuilder.appendRow(i, j, 0);
                }
                else {
                    previousLeftPos = i;
                    middleRightPos = j;
                    Page res = pageBuilder.buildPage();
                    pageBuilder.reset();
                    return res;
                }
            }
        }

        // calculate left and right range for equal positions
        while (true) {
            while ((leftPos < leftSortedPagesIndex.getPositionCount()) &&
                    (rightPos < rightSortedPagesIndex.getPositionCount())) {
                long compareResult = compareJoinPosition();
                if (compareResult > 0) {
                    rightPos += 1;
                }
                else if (compareResult < 0) {
                    leftPos += 1;
                }
                else {
                    break;
                }
            }
            if ((leftPos >= leftSortedPagesIndex.getPositionCount()) ||
                    (rightPos >= rightSortedPagesIndex.getPositionCount())) {
                this.finished = true;
                if (!pageBuilder.isEmpty()) {
                    Page res = pageBuilder.buildPage();
                    pageBuilder.reset();
                    return res;
                }
                return null;
            }
            if (compareJoinPosition() != 0) {
                throw new IllegalStateException();
            }
            previousLeftPos = leftPos;
            previousRightPos = rightPos;
            while (leftPos < leftSortedPagesIndex.getPositionCount() && leftPositionEqual(previousLeftPos, leftPos)) {
                leftPos = leftPos + 1;
            }
            while (rightPos < rightSortedPagesIndex.getPositionCount() && rightPositionEqual(previousRightPos, rightPos)) {
                rightPos = rightPos + 1;
            }
            for (int i = previousLeftPos; i < leftPos; i++) {
                for (int j = previousRightPos; j < rightPos; j++) {
                    if (!pageBuilder.isFull()) {
                        pageBuilder.appendRow(i, j, 0);
                    }
                    else {
                        previousLeftPos = i;
                        middleRightPos = j;
                        Page res = pageBuilder.buildPage();
                        pageBuilder.reset();
                        return res;
                    }
                }
            }
        }
    }

    private boolean leftPositionEqual(int pos1, int pos2)
    {
        return leftPagesIndexComparator.compareTo(leftSortedPagesIndex, pos1, pos2) == 0;
    }

    private boolean rightPositionEqual(int pos1, int pos2)
    {
        return rightPagesIndexComparator.compareTo(rightSortedPagesIndex, pos1, pos2) == 0;
    }

    private long compareJoinPosition()
    {
        LongArrayList leftAddresses = leftSortedPagesIndex.getValueAddresses();
        LongArrayList rightAddresses = rightSortedPagesIndex.getValueAddresses();

        long leftPageAddress = leftAddresses.getLong(leftPos);
        long rightPageAddress = rightAddresses.getLong(rightPos);
        int leftPageIndex = decodeSliceIndex(leftPageAddress);
        int leftPagePosition = decodePosition(leftPageAddress);
        int rightPageIndex = decodeSliceIndex(rightPageAddress);
        int rightPagePosition = decodePosition(rightPageAddress);

        for (int i = 0; i < leftMergeChannels.size(); i++) {
            BlockTypeOperators.BlockPositionComparison comparisonOperator = joinEqualOperators.get(i);
            Block leftBlock = leftSortedPagesIndex.getChannel(leftMergeChannels.get(i)).get(leftPageIndex);
            Block rightBlock = rightSortedPagesIndex.getChannel(rightMergeChannels.get(i)).get(rightPageIndex);
            long compareResult = comparisonOperator.compare(leftBlock, leftPagePosition, rightBlock, rightPagePosition);
            if (compareResult != 0) {
                return compareResult;
            }
        }
        return 0;
    }

    @Override
    public void close()
    {
        leftSortedPagesIndex.clear();
        rightSortedPagesIndex.clear();
    }
}
