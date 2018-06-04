/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.query.routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinsTree;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.rules.RemoveBlackoutRealizationsRule;
import org.apache.kylin.util.JoinsGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class RealizationChooser {

    private static final Logger logger = LoggerFactory.getLogger(RealizationChooser.class);

    // select models for given contexts, return realization candidates for each context
    public static void selectRealization(List<OLAPContext> contexts) {
        // try different model for different context
        for (OLAPContext ctx : contexts) {
            ctx.realizationCheck = new RealizationCheck();
            attemptSelectRealization(ctx);
            Preconditions.checkNotNull(ctx.realization);
        }
    }

    private static void attemptSelectRealization(OLAPContext context) {
        // step1: collect realization info
        Map<DataModelDesc, Set<IRealization>> modelMap = makeOrderedModelMap(context);
        if (modelMap.size() == 0) {
            throw new NoRealizationFoundException("No model found for " + toErrorMsg(context));
        }

        //check all models to collect error message, just for check
        if (BackdoorToggles.getCheckAllModels()) {
            for (Map.Entry<DataModelDesc, Set<IRealization>> entry : modelMap.entrySet()) {
                final DataModelDesc model = entry.getKey();
                final Map<String, String> aliasMap = matches(model, context);
                if (aliasMap != null) {
                    context.fixModel(model, aliasMap);
                    QueryRouter.selectRealization(context, entry.getValue());
                    context.unfixModel();
                }
            }
        }

        // step2: model match
        Set<IRealization> orderedRealizations = null;
        final Map<DataModelDesc, Map<String, String>> model2aliasMap = Maps.newHashMap();
        for (Map.Entry<DataModelDesc, Set<IRealization>> entry : modelMap.entrySet()) {
            final DataModelDesc model = entry.getKey();
            Map<String, String> aliasMap = matches(model, context);
            if (aliasMap != null) {
                model2aliasMap.put(model, aliasMap);
                if (orderedRealizations == null) {
                    orderedRealizations = sortRealizationByCostAndType(context, entry.getValue());
                } else {
                    orderedRealizations.addAll(sortRealizationByCostAndType(context, entry.getValue()));
                }
            }
        }

        // step3: select realization order by cost
        if (orderedRealizations == null)
            throw new NoRealizationFoundException("No realization found for " + toErrorMsg(context));
        for (IRealization realization : orderedRealizations) {
            context.fixModel(realization.getModel(), model2aliasMap.get(realization.getModel()));
            IRealization candidate = QueryRouter.selectRealization(context, Sets.newHashSet(realization));
            if (candidate == null) {
                context.unfixModel();
                continue;
            }
            context.realization = candidate;
            return;
        }
        throw new NoRealizationFoundException("No realization found for " + toErrorMsg(context));
    }

    private static String toErrorMsg(OLAPContext ctx) {
        StringBuilder buf = new StringBuilder("OLAPContext");
        RealizationCheck checkResult = ctx.realizationCheck;
        for (RealizationCheck.IncapableReason reason : checkResult.getCubeIncapableReasons().values()) {
            buf.append(", ").append(reason);
        }
        for (List<RealizationCheck.IncapableReason> reasons : checkResult.getModelIncapableReasons().values()) {
            for (RealizationCheck.IncapableReason reason : reasons) {
                buf.append(", ").append(reason);
            }
        }
        buf.append(", ").append(ctx.firstTableScan);
        for (JoinDesc join : ctx.joins)
            buf.append(", ").append(join);
        return buf.toString();
    }

    private static Map<String, String> matches(DataModelDesc model, OLAPContext ctx) {
        Map<String, String> matchUp = Maps.newHashMap();
        TableRef firstTable = ctx.firstTableScan.getTableRef();
        boolean matched;

        if (ctx.joins.isEmpty() && model.isLookupTable(firstTable.getTableIdentity())) {
            // one lookup table
            String modelAlias = model.findFirstTable(firstTable.getTableIdentity()).getAlias();
            matchUp = ImmutableMap.of(firstTable.getAlias(), modelAlias);
            matched = true;
        } else if (ctx.joins.size() != ctx.allTableScans.size() - 1) {
            // has hanging tables
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_BAD_JOIN_SEQUENCE));
            throw new IllegalStateException("Please adjust the sequence of join tables. " + toErrorMsg(ctx));
        } else {
            // normal big joins
            if (ctx.joinsGraph == null) {
                ctx.joinsGraph = new JoinsGraph(firstTable, ctx.joins);
            }
            // TODO: finally we should remove joinsTree, it's only used in Auto-Modelling.
            if (ctx.joinsTree == null) {
                Pair<TableRef, List<JoinDesc>> reordered = reorderJoins(firstTable, ctx.joins);
                ctx.joinsTree = new JoinsTree(reordered.getFirst(), reordered.getSecond());
            }
            matched = JoinsGraph.match(ctx.joinsGraph, model.getJoinsGraph(), matchUp);
        }

        if (!matched) {
            ctx.realizationCheck.addModelIncapableReason(model,
                    RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.MODEL_UNMATCHED_JOIN));
            return null;
        }
        ctx.realizationCheck.addCapableModel(model, matchUp);
        return matchUp;
    }

    private static Pair<TableRef, List<JoinDesc>> reorderJoins(TableRef root, List<JoinDesc> joins) {
        if (joins.isEmpty()) {
            return new Pair(root, joins);
        }

        Set<TableRef> fkSides = new HashSet<>();
        Set<TableRef> pkSides = new HashSet<>();
        Map<TableRef, List<JoinDesc>> fkMap = new HashMap<>();
        for (JoinDesc join : joins) {
            fkSides.add(join.getFKSide());
            pkSides.add(join.getPKSide());
            if (fkMap.containsKey(join.getFKSide())) {
                fkMap.get(join.getFKSide()).add(join);
            } else {
                fkMap.put(join.getFKSide(), Lists.newArrayList(join));
            }
        }
        fkSides.removeAll(pkSides);
        if (fkSides.size() > 1) {
            throw new IllegalArgumentException("joins is not a valid join tree: " + joins);
        }

        TableRef newRoot = fkSides.iterator().next();
        Queue<TableRef> pending = new LinkedList();
        pending.offer(newRoot);
        List<JoinDesc> reordered = new ArrayList<>();

        while (!pending.isEmpty()) {
            TableRef cur = pending.poll();
            if (fkMap.containsKey(cur)) {
                reordered.addAll(fkMap.get(cur));
                for (JoinDesc join : fkMap.get(cur)) {
                    pending.offer(join.getPKSide());
                }
            }
        }

        return new Pair(newRoot, reordered);
    }

    private static Map<DataModelDesc, Set<IRealization>> makeOrderedModelMap(OLAPContext context) {
        OLAPContext first = context;
        KylinConfig kylinConfig = first.olapSchema.getConfig();
        String projectName = first.olapSchema.getProjectName();
        String tableName = first.firstTableScan.getOlapTable().getTableName();
        Set<IRealization> realizations = ProjectManager.getInstance(kylinConfig).getRealizationsByTable(projectName,
                tableName);

        final Map<DataModelDesc, Set<IRealization>> model2Realizations = Maps.newHashMap();
        for (IRealization real : realizations) {
            if (real.isReady() == false) {
                context.realizationCheck.addIncapableCube(real,
                        RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_READY));
                continue;
            }
            if (containsAll(real.getAllColumnDescs(), first.allColumns) == false) {
                context.realizationCheck.addIncapableCube(real, RealizationCheck.IncapableReason
                        .notContainAllColumn(notContain(real.getAllColumnDescs(), first.allColumns)));
                continue;
            }
            if (RemoveBlackoutRealizationsRule.accept(real) == false) {
                context.realizationCheck.addIncapableCube(real, RealizationCheck.IncapableReason
                        .create(RealizationCheck.IncapableType.CUBE_BLACK_OUT_REALIZATION));
                continue;
            }
            if (model2Realizations.get(real.getModel()) == null || model2Realizations.size() == 0) {
                model2Realizations.put(real.getModel(), Sets.<IRealization> newHashSet(real));
            } else {
                model2Realizations.get(real.getModel()).add(real);
            }
        }

        return model2Realizations;
    }

    private static boolean containsAll(Set<ColumnDesc> allColumnDescs, Set<TblColRef> allColumns) {
        for (TblColRef col : allColumns) {
            if (allColumnDescs.contains(col.getColumnDesc()) == false)
                return false;
        }
        return true;
    }

    private static List<TblColRef> notContain(Set<ColumnDesc> allColumnDescs, Set<TblColRef> allColumns) {
        List<TblColRef> notContainCols = Lists.newArrayList();
        for (TblColRef col : allColumns) {
            if (!allColumnDescs.contains(col.getColumnDesc()))
                notContainCols.add(col);
        }
        return notContainCols;
    }

    private static Set<IRealization> sortRealizationByCostAndType(final OLAPContext context,
            Collection<? extends IRealization> realizations) {
        final Map<RealizationType, Integer> weightOfRealization = Maps.newHashMap(Candidate.PRIORITIES);
        if (context.hasAgg) {
            weightOfRealization.put(RealizationType.INVERTED_INDEX,
                    weightOfRealization.get(RealizationType.INVERTED_INDEX) + 1);
        } else {
            weightOfRealization.put(RealizationType.CUBE, weightOfRealization.get(RealizationType.CUBE) + 1);
        }
        Set<IRealization> orderedRealizations = Sets.newTreeSet(new Comparator<IRealization>() {
            @Override
            public int compare(IRealization o1, IRealization o2) {
                int res = weightOfRealization.get(o1.getType()) - weightOfRealization.get(o2.getType()) == 0
                        ? o1.getCost(context.getSQLDigest()) - o2.getCost(context.getSQLDigest())
                        : weightOfRealization.get(o1.getType()) - weightOfRealization.get(o2.getType());
                return res == 0 ? o1.getName().compareTo(o2.getName()) : res;
            }
        });

        orderedRealizations.addAll(realizations);
        return orderedRealizations;
    }

}
