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
package io.trino.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinType.FULL;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static java.util.Objects.requireNonNull;

@Immutable
public class AdaptiveJoinNode
        extends PlanNode
{
    private final JoinType type;
    private final PlanNode build;
    private final PlanNode outer;
    private final List<JoinNode.EquiJoinClause> criteria;
    private final List<Symbol> buildOutputSymbols;
    private final List<Symbol> probeOutputSymbols;
    private final List<Symbol> probePrimaryKeySymbols;
    private final List<Symbol> outerLeftSymbols;
    private final List<Symbol> outerRightSymbols;
    private final boolean maySkipOutputDuplicates;
    private final Optional<Expression> filter;
    private final Optional<Symbol> buildHashSymbol;
    private final Optional<Symbol> outerHashSymbol;
    private final Optional<JoinNode.DistributionType> distributionType;
    private final Optional<Boolean> spillable;
    private final Map<DynamicFilterId, Symbol> dynamicFilters;

    // stats and cost used for join reordering
    private final Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost;

    @JsonCreator
    public AdaptiveJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") JoinType type,
            @JsonProperty("build") PlanNode build,
            @JsonProperty("outer") PlanNode outer,
            @JsonProperty("criteria") List<JoinNode.EquiJoinClause> criteria,
            @JsonProperty("buildOutputSymbols") List<Symbol> buildOutputSymbols,
            @JsonProperty("probeOutputSymbols") List<Symbol> probeOutputSymbols,
            @JsonProperty("probePrimaryKeySymbols") List<Symbol> probePrimaryKeySymbols,
            @JsonProperty("outerLeftSymbols") List<Symbol> outerLeftSymbols,
            @JsonProperty("outerRightSymbols") List<Symbol> outerRightSymbols,
            @JsonProperty("maySkipOutputDuplicates") boolean maySkipOutputDuplicates,
            @JsonProperty("filter") Optional<Expression> filter,
            @JsonProperty("buildHashSymbol") Optional<Symbol> buildHashSymbol,
            @JsonProperty("outerHashSymbol") Optional<Symbol> outerHashSymbol,
            @JsonProperty("distributionType") Optional<JoinNode.DistributionType> distributionType,
            @JsonProperty("spillable") Optional<Boolean> spillable,
            @JsonProperty("dynamicFilters") Map<DynamicFilterId, Symbol> dynamicFilters,
            @JsonProperty("reorderJoinStatsAndCost") Optional<PlanNodeStatsAndCostSummary> reorderJoinStatsAndCost)
    {
        super(id);
        requireNonNull(type, "type is null");
        requireNonNull(build, "left is null");
        requireNonNull(outer, "right is null");
        requireNonNull(criteria, "criteria is null");
        requireNonNull(buildOutputSymbols, "leftOutputSymbols is null");
        requireNonNull(probeOutputSymbols, "probeOutputSymbols is null");
        requireNonNull(probePrimaryKeySymbols, "probePrimaryKeySymbols is null");
        requireNonNull(outerLeftSymbols, "rightOutputSymbols is null");
        requireNonNull(outerRightSymbols, "rightOutputSymbols is null");
        requireNonNull(filter, "filter is null");
        requireNonNull(buildHashSymbol, "leftHashSymbol is null");
        requireNonNull(outerHashSymbol, "outerHashSymbol is null");
        requireNonNull(distributionType, "distributionType is null");
        requireNonNull(spillable, "spillable is null");

        this.type = type;
        this.build = build;
        this.outer = outer;
        this.criteria = ImmutableList.copyOf(criteria);
        this.buildOutputSymbols = ImmutableList.copyOf(buildOutputSymbols);
        this.probeOutputSymbols = ImmutableList.copyOf(probeOutputSymbols);
        this.probePrimaryKeySymbols = ImmutableList.copyOf(probePrimaryKeySymbols);
        this.outerLeftSymbols = ImmutableList.copyOf(outerLeftSymbols);
        this.outerRightSymbols = ImmutableList.copyOf(outerRightSymbols);
        this.maySkipOutputDuplicates = maySkipOutputDuplicates;
        this.filter = filter;
        this.buildHashSymbol = buildHashSymbol;
        this.outerHashSymbol = outerHashSymbol;
        this.distributionType = distributionType;
        this.spillable = spillable;
        this.dynamicFilters = ImmutableMap.copyOf(requireNonNull(dynamicFilters, "dynamicFilters is null"));
        this.reorderJoinStatsAndCost = requireNonNull(reorderJoinStatsAndCost, "reorderJoinStatsAndCost is null");

        Set<Symbol> buildSymbols = ImmutableSet.copyOf(build.getOutputSymbols());
        Set<Symbol> outerSymbols = ImmutableSet.copyOf(outer.getOutputSymbols());

        checkArgument(buildSymbols.containsAll(buildOutputSymbols), "Left source inputs do not contain all left output symbols");

        checkArgument(!(criteria.isEmpty() && buildHashSymbol.isPresent()), "Left hash symbol is only valid in an equijoin");
        checkArgument(!(criteria.isEmpty() && outerHashSymbol.isPresent()), "Right hash symbol is only valid in an equijoin");

        criteria.forEach(equiJoinClause ->
                checkArgument(
                        outerSymbols.contains(equiJoinClause.getLeft()) &&
                                buildSymbols.contains(equiJoinClause.getRight()),
                        "Equality join criteria should be normalized according to join sides: %s", equiJoinClause));

        if (distributionType.isPresent()) {
            // The implementation of full outer join only works if the data is hash partitioned.
            checkArgument(
                    !(distributionType.get() == REPLICATED && (type == RIGHT || type == FULL)),
                    "%s join do not work with %s distribution type",
                    type,
                    distributionType.get());
        }

        for (Symbol symbol : dynamicFilters.values()) {
            checkArgument(buildSymbols.contains(symbol), "Right join input doesn't contain symbol for dynamic filter: %s", symbol);
        }
    }

    @JsonProperty("type")
    public JoinType getType()
    {
        return type;
    }

    @JsonProperty("outer")
    public PlanNode getOuter()
    {
        return outer;
    }

    @JsonProperty("build")
    public PlanNode getBuild()
    {
        return build;
    }

    @JsonProperty("criteria")
    public List<JoinNode.EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @JsonProperty("probeOutputSymbols")
    public List<Symbol> getProbeOutputSymbols()
    {
        return probeOutputSymbols;
    }

    @JsonProperty("probePrimaryKeySymbols")
    public List<Symbol> getProbePrimaryKeySymbols()
    {
        return probePrimaryKeySymbols;
    }

    @JsonProperty("buildOutputSymbols")
    public List<Symbol> getBuildOutputSymbols()
    {
        return buildOutputSymbols;
    }

    @JsonProperty("outerLeftSymbols")
    public List<Symbol> getOuterLeftSymbols()
    {
        return outerLeftSymbols;
    }

    @JsonProperty("outerRightSymbols")
    public List<Symbol> getOuterRightSymbols()
    {
        return outerRightSymbols;
    }

    @JsonProperty("filter")
    public Optional<Expression> getFilter()
    {
        return filter;
    }

    @JsonProperty("outerHashSymbol")
    public Optional<Symbol> getOuterHashSymbol()
    {
        return outerHashSymbol;
    }

    @JsonProperty("buildHashSymbol")
    public Optional<Symbol> getBuildHashSymbol()
    {
        return buildHashSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(build, outer);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(probeOutputSymbols)
                .addAll(buildOutputSymbols)
                .build();
    }

    @JsonProperty("distributionType")
    public Optional<JoinNode.DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @JsonProperty("spillable")
    public Optional<Boolean> isSpillable()
    {
        return spillable;
    }

    @JsonProperty("maySkipOutputDuplicates")
    public boolean isMaySkipOutputDuplicates()
    {
        return maySkipOutputDuplicates;
    }

    @JsonProperty
    public Map<DynamicFilterId, Symbol> getDynamicFilters()
    {
        return dynamicFilters;
    }

    @JsonProperty
    public Optional<PlanNodeStatsAndCostSummary> getReorderJoinStatsAndCost()
    {
        return reorderJoinStatsAndCost;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitAdaptiveJoin(this, context);
    }

    @Override
    public AdaptiveJoinNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new AdaptiveJoinNode(getId(), type, newChildren.get(0), newChildren.get(1), criteria,
                buildOutputSymbols, probeOutputSymbols, probePrimaryKeySymbols, outerLeftSymbols, outerRightSymbols, maySkipOutputDuplicates,
                filter, buildHashSymbol, outerHashSymbol, distributionType, spillable, dynamicFilters, reorderJoinStatsAndCost);
    }

    public AdaptiveJoinNode withDistributionType(JoinNode.DistributionType distributionType)
    {
        throw new UnsupportedOperationException();
    }

    public boolean isCrossJoin()
    {
        return criteria.isEmpty() && filter.isEmpty() && type == INNER;
    }
}
