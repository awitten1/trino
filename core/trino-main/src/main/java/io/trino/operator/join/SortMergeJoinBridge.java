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

import com.google.common.util.concurrent.SettableFuture;
import io.trino.operator.PagesIndex;
import io.trino.spi.Page;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SortMergeJoinBridge
{
    private final List<PagesIndex> leftUpPagesIndexList;
    private final List<PagesIndex> leftDownInputList;
    private final List<PagesIndex> rightUpPagesIndexList;
    private final List<PagesIndex> rightDownInputList;
    private final List<List<Page>> leftJoinResults;
    private final List<SettableFuture<Void>> sortFinishedFutureList;
    private final List<AtomicInteger> sortFinishedCntList;

    private int upSortedPagesIdx;
    private int downSortedPagesIdx;
    private int sortedFutureIdx;
    private int leftJoinResultIdx;

    public SortMergeJoinBridge(
            int size,
            List<Type> leftSourceTypes,
            List<Type> rightSourceTypes,
            PagesIndex.Factory pagesIndexFactory,
            int expectedPositions,
            List<Integer> leftChannels,
            List<Integer> rightChannels,
            List<SortOrder> sortOrder,
            int pagesBatchSize)
    {
        leftUpPagesIndexList = new ArrayList<>();
        leftDownInputList = new ArrayList<>();
        rightUpPagesIndexList = new ArrayList<>();
        rightDownInputList = new ArrayList<>();
        sortFinishedFutureList = new ArrayList<>();
        sortFinishedCntList = new ArrayList<>();
        leftJoinResults = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            if (leftChannels != null) {
                leftUpPagesIndexList.add(pagesIndexFactory.newPagesIndex(leftSourceTypes, expectedPositions, pagesBatchSize, leftChannels, sortOrder));
                leftDownInputList.add(pagesIndexFactory.newPagesIndex(leftSourceTypes, expectedPositions, pagesBatchSize, leftChannels, sortOrder));
            }
            else {
                leftUpPagesIndexList.add(pagesIndexFactory.newPagesIndex(leftSourceTypes, expectedPositions, pagesBatchSize, leftChannels, sortOrder));
                leftDownInputList.add(pagesIndexFactory.newPagesIndex(leftSourceTypes, expectedPositions, pagesBatchSize, leftChannels, sortOrder));
            }
            if (rightChannels != null) {
                rightUpPagesIndexList.add(pagesIndexFactory.newPagesIndex(rightSourceTypes, expectedPositions, pagesBatchSize, rightChannels, sortOrder));
                rightDownInputList.add(pagesIndexFactory.newPagesIndex(rightSourceTypes, expectedPositions, pagesBatchSize, rightChannels, sortOrder));
            }
            else {
                rightUpPagesIndexList.add(pagesIndexFactory.newPagesIndex(rightSourceTypes, expectedPositions, pagesBatchSize, rightChannels, sortOrder));
                rightDownInputList.add(pagesIndexFactory.newPagesIndex(rightSourceTypes, expectedPositions, pagesBatchSize, rightChannels, sortOrder));
            }
            leftJoinResults.add(new LinkedList<>());
            sortFinishedFutureList.add(SettableFuture.create());
            sortFinishedCntList.add(new AtomicInteger(0));
        }
        upSortedPagesIdx = 0;
        downSortedPagesIdx = 0;
        sortedFutureIdx = 0;
        leftJoinResultIdx = 0;
    }

    public PagesIndex getLeftUpPagesIndex(int pos)
    {
        return leftUpPagesIndexList.get(pos);
    }

    public PagesIndex getLeftDownPagesIndex(int pos)
    {
        return leftDownInputList.get(pos);
    }

    public PagesIndex getRightUpPagesIndex(int pos)
    {
        return rightUpPagesIndexList.get(pos);
    }

    public PagesIndex getRightDownPagesIndex(int pos)
    {
        return rightDownInputList.get(pos);
    }

    public List<Page> getLeftJoinResult(int pos)
    {
        return leftJoinResults.get(pos);
    }

    public SettableFuture<Void> getSortFinishedFuture(int pos)
    {
        return sortFinishedFutureList.get(pos);
    }

    public AtomicInteger getSortFinishedCnt(int pos)
    {
        return sortFinishedCntList.get(pos);
    }

    public List<PagesIndex> getNextUpSortedPagesPair()
    {
        List<PagesIndex> res = new ArrayList<>();
        res.add(leftUpPagesIndexList.get(upSortedPagesIdx));
        res.add(rightUpPagesIndexList.get(upSortedPagesIdx));
        upSortedPagesIdx += 1;
        return res;
    }

    public List<PagesIndex> getNextDownSortedPagesPair()
    {
        List<PagesIndex> res = new ArrayList<>();
        res.add(leftDownInputList.get(downSortedPagesIdx));
        res.add(rightDownInputList.get(downSortedPagesIdx));
        downSortedPagesIdx += 1;
        return res;
    }

    public List<Page> getNextLeftJoinResult()
    {
        List<Page> res = leftJoinResults.get(leftJoinResultIdx);
        leftJoinResultIdx += 1;
        return res;
    }

    public SettableFuture<Void> getNextFinishedFuture()
    {
        SettableFuture<Void> finishedFuture = sortFinishedFutureList.get(sortedFutureIdx);
        sortedFutureIdx += 1;
        return finishedFuture;
    }
}
