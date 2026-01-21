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
import io.trino.operator.PagesIndex;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;

import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;

public class SortMergePageBuilder
{
    private final PagesIndex leftPagesIndex;
    private final PagesIndex rightPagesIndex;
    private final List<Integer> leftOutputChannels;
    private final List<Integer> rightOutputChannels;
    private final PageBuilder pageBuilder;

    public SortMergePageBuilder(
            PagesIndex leftPagesIndex,
            PagesIndex rightPagesIndex,
            List<Type> leftOutputTypes,
            List<Type> rightOutputTypes,
            List<Integer> leftOutputChannels,
            List<Integer> rightOutputChannels)
    {
        this.leftPagesIndex = leftPagesIndex;
        this.rightPagesIndex = rightPagesIndex;
        this.leftOutputChannels = leftOutputChannels;
        this.rightOutputChannels = rightOutputChannels;
        List<Type> outputTypes = ImmutableList.<Type>builder().addAll(leftOutputTypes).addAll(rightOutputTypes).build();
        this.pageBuilder = new PageBuilder(outputTypes);
    }

    public boolean isFull()
    {
        return pageBuilder.isFull();
    }

    public boolean isEmpty()
    {
        return pageBuilder.isEmpty();
    }

    public void appendRow(int leftPos, int rightPos, int outputChannelOffset)
    {
        LongArrayList leftAddresses = leftPagesIndex.getValueAddresses();
        LongArrayList rightAddresses = rightPagesIndex.getValueAddresses();
        int leftPageIndex = decodeSliceIndex(leftAddresses.getLong(leftPos));
        int leftPagePosition = decodePosition(leftAddresses.getLong(leftPos));
        int rightPageIndex = decodeSliceIndex(rightAddresses.getLong(rightPos));
        int rightPagePosition = decodePosition(rightAddresses.getLong(rightPos));

        pageBuilder.declarePosition();
        for (int leftOutputIndex : leftOutputChannels) {
            Block block = leftPagesIndex.getChannel(leftOutputIndex).get(leftPageIndex);
            pageBuilder.getBlockBuilder(outputChannelOffset).appendBlockRange(block, leftPagePosition, 1);
            outputChannelOffset++;
        }

        for (int rightOutputIndex : rightOutputChannels) {
            Block block = rightPagesIndex.getChannel(rightOutputIndex).get(rightPageIndex);
            pageBuilder.getBlockBuilder(outputChannelOffset).appendBlockRange(block, rightPagePosition, 1);
            outputChannelOffset++;
        }
    }

    public void reset()
    {
        pageBuilder.reset();
    }

    public Page buildPage()
    {
        return pageBuilder.build();
    }
}
