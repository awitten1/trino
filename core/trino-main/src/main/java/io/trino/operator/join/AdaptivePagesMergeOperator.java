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
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.type.BlockTypeOperators;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static java.util.Objects.requireNonNull;

public class AdaptivePagesMergeOperator
        implements Operator
{
    public static class AdaptivePagesMergeOperatorFactory
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

        public AdaptivePagesMergeOperatorFactory(
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
            this.leftTypes = requireNonNull(leftTypes, "leftTypes is null");
            this.rightTypes = requireNonNull(rightTypes, "rightTypes is null");
            this.leftMergeChannels = requireNonNull(leftMergeChannels, "leftMergeChannels is null");
            this.rightMergeChannels = requireNonNull(rightMergeChannels, "rightMergeChannels is null");
            this.leftOutputChannels = requireNonNull(leftOutputChannels, "leftOutputChannels is null");
            this.rightOutputChannels = requireNonNull(rightOutputChannels, "rightOutputChannels is null");
            this.joinEqualOperators = requireNonNull(joinEqualOperators, "joinEqualOperators is null");
            this.bridge = requireNonNull(bridge, "bridge is null");
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
            List<Page> leftJoinResult = bridge.getNextLeftJoinResult();
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
            return new AdaptivePagesMergeOperator(
                    operatorContext,
                    leftMergeChannels,
                    rightMergeChannels,
                    leftPagesIndex,
                    rightPagesIndex,
                    leftJoinResult,
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

    private final List<Page> leftJoinResult;
    private final PagesMergeOperator pagesMergeOperator;

    public AdaptivePagesMergeOperator(
            OperatorContext operatorContext,
            List<Integer> leftMergeChannels,
            List<Integer> rightMergeChannels,
            PagesIndex leftSortedPagesIndex,
            PagesIndex rightSortedPagesIndex,
            List<Page> leftJoinResult,
            PagesIndexComparator leftPagesIndexComparator,
            PagesIndexComparator rightPagesIndexComparator,
            SettableFuture<Void> sortFinishedFuture,
            List<BlockTypeOperators.BlockPositionComparison> joinEqualOperators,
            SortMergePageBuilder pageBuilder)
    {
        pagesMergeOperator = new PagesMergeOperator(
                operatorContext,
                leftMergeChannels,
                rightMergeChannels,
                leftSortedPagesIndex,
                rightSortedPagesIndex,
                leftPagesIndexComparator,
                rightPagesIndexComparator,
                sortFinishedFuture,
                joinEqualOperators,
                pageBuilder);
        this.leftJoinResult = requireNonNull(leftJoinResult, "leftJoinResult is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return pagesMergeOperator.getOperatorContext();
    }

    @Override
    public void finish()
    {
        pagesMergeOperator.finish();
    }

    @Override
    public boolean isFinished()
    {
        return pagesMergeOperator.isFinished() && leftJoinResult.isEmpty();
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return pagesMergeOperator.isBlocked();
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
        if (!leftJoinResult.isEmpty()) {
            return leftJoinResult.remove(0);
        }
        return pagesMergeOperator.getOutput();
    }

    @Override
    public void close()
    {
        pagesMergeOperator.close();
    }
}
