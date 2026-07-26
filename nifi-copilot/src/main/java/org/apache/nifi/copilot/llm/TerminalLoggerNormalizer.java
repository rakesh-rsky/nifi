/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.copilot.llm;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normalizes terminal logger (LogAttribute / LogMessage) partitioning within a generated NiFi
 * flow specification before deployment.
 *
 * <p>Two phases are applied in order:
 * <ol>
 *   <li><b>Split:</b> when a shared logger receives connections from sources that are sequential
 *       (one reachable from another), at different pipeline ranks, or in disconnected flow
 *       components, each structural-sibling source-group gets its own logger clone.
 *   <li><b>Consolidate:</b> when the model generated one logger per parallel worker for the same
 *       semantic outcome and identical processor type/configuration, those loggers are merged into
 *       a single shared logger.
 * </ol>
 *
 * <p>Only processors present in the generated spec are modified.  References to canvas processors
 * absent from the generated list are never changed.  No funnels are created.
 */
class TerminalLoggerNormalizer {

    private static final Logger log = LoggerFactory.getLogger(TerminalLoggerNormalizer.class);

    private static final Set<String> SUCCESS_RELS = Set.of("success", "response", "matched");
    private static final Set<String> FAILURE_RELS = Set.of("failure", "no retry", "retry", "unmatched");

    // Fields excluded from processor equivalence during consolidation (identity / presentation / control)
    private static final Set<String> IDENTITY_FIELDS =
            Set.of("id", "name", "x", "y", "preserve_separate_terminal", "type");

    enum Outcome { SUCCESS, FAILURE, MIXED }

    // -------------------------------------------------------------------------
    // Public entry point
    // -------------------------------------------------------------------------

    /**
     * Normalizes terminal logger partitioning in-place.
     *
     * @param processors        mutable generated processor list; may grow when loggers are cloned
     * @param terminalLoggerIds mutable set of terminal-logger IDs; updated as loggers are added or removed
     * @param connections       mutable generated connection list; connections are redirected in-place
     */
    void normalize(
            final List<Map<String, Object>> processors,
            final Set<String> terminalLoggerIds,
            final List<Map<String, Object>> connections) {

        if (processors.isEmpty() || terminalLoggerIds.isEmpty()) {
            return;
        }

        // Index of all generated processor IDs (used to ignore external canvas references)
        final Set<String> generatedIds = processors.stream()
                .map(p -> String.valueOf(p.get("id")))
                .collect(Collectors.toCollection(LinkedHashSet::new));

        final Map<String, Map<String, Object>> byId = new LinkedHashMap<>();
        processors.forEach(p -> byId.put(String.valueOf(p.get("id")), p));

        // Clean adjacency graph: no self-loops, no edges from/to terminal loggers, generated only
        final Map<String, Set<String>> cleanGraph =
                buildCleanGraph(connections, terminalLoggerIds, generatedIds);

        // Fix 2: SCC-aware rank computation so nodes downstream from cycles get correct depths
        final Map<String, Integer> ranks = computeRanks(cleanGraph, generatedIds);

        // Loggers that result from a structural split must not be re-merged by consolidation
        final Set<String> splitProtectedIds = new HashSet<>();

        // Phase 1: split loggers shared across sequential or non-adjacent source stages
        splitLoggers(processors, byId, terminalLoggerIds, connections,
                cleanGraph, ranks, generatedIds, splitProtectedIds);

        // Phase 2: consolidate one-per-worker loggers that serve the same semantic outcome
        consolidateLoggers(processors, byId, terminalLoggerIds, connections,
                cleanGraph, ranks, generatedIds, splitProtectedIds);
    }

    // -------------------------------------------------------------------------
    // Graph construction
    // -------------------------------------------------------------------------

    /**
     * Builds a clean adjacency map for reachability and rank analysis.
     * Excluded: self-loops, edges from terminal loggers, edges whose destination is a terminal
     * logger (prevents reachability propagating through sinks), and edges involving processors
     * absent from the generated list.
     */
    private Map<String, Set<String>> buildCleanGraph(
            final List<Map<String, Object>> connections,
            final Set<String> loggerIds,
            final Set<String> generatedIds) {
        final Map<String, Set<String>> graph = new HashMap<>();
        for (final Map<String, Object> conn : connections) {
            final String from = String.valueOf(conn.get("from"));
            final String to   = String.valueOf(conn.get("to"));
            if (from.equals(to)) continue;
            if (loggerIds.contains(from)) continue;
            if (loggerIds.contains(to)) continue;
            if (!generatedIds.contains(from) || !generatedIds.contains(to)) continue;
            graph.computeIfAbsent(from, k -> new HashSet<>()).add(to);
        }
        return graph;
    }

    // -------------------------------------------------------------------------
    // Fix 2: SCC-aware rank computation — Tarjan's condensation + Kahn's on DAG
    // -------------------------------------------------------------------------

    private Map<String, Integer> computeRanks(
            final Map<String, Set<String>> graph,
            final Set<String> allNodes) {

        // Step 1: find all SCCs (cycles become single super-nodes)
        final List<List<String>> sccs = findSCCs(graph, allNodes);

        // Step 2: map each node to its SCC index
        final Map<String, Integer> nodeToScc = new HashMap<>();
        for (int i = 0; i < sccs.size(); i++) {
            for (final String node : sccs.get(i)) {
                nodeToScc.put(node, i);
            }
        }

        // Step 3: build condensed DAG between SCC super-nodes
        final int numSccs = sccs.size();
        final Map<Integer, Set<Integer>> condensed = new HashMap<>();
        final Map<Integer, Integer> condensedInDeg = new HashMap<>();
        for (int i = 0; i < numSccs; i++) {
            condensed.put(i, new HashSet<>());
            condensedInDeg.put(i, 0);
        }
        for (final Map.Entry<String, Set<String>> e : graph.entrySet()) {
            final int fromScc = nodeToScc.getOrDefault(e.getKey(), -1);
            for (final String dst : e.getValue()) {
                final int toScc = nodeToScc.getOrDefault(dst, -1);
                if (fromScc >= 0 && toScc >= 0 && fromScc != toScc) {
                    if (condensed.get(fromScc).add(toScc)) {
                        condensedInDeg.merge(toScc, 1, Integer::sum);
                    }
                }
            }
        }

        // Step 4: longest-path Kahn's on the condensed DAG
        final Map<Integer, Integer> sccRank = new HashMap<>();
        final ArrayDeque<Integer> queue = new ArrayDeque<>();
        final Map<Integer, Integer> remaining = new HashMap<>(condensedInDeg);
        for (int i = 0; i < numSccs; i++) {
            if (remaining.getOrDefault(i, 0) == 0) {
                sccRank.put(i, 0);
                queue.add(i);
            }
        }
        while (!queue.isEmpty()) {
            final int cur = queue.poll();
            final int curRank = sccRank.getOrDefault(cur, 0);
            for (final int next : condensed.getOrDefault(cur, Set.of())) {
                final int newRank = curRank + 1;
                if (newRank > sccRank.getOrDefault(next, -1)) {
                    sccRank.put(next, newRank);
                }
                final int rem = remaining.merge(next, -1, Integer::sum);
                if (rem == 0) {
                    queue.add(next);
                }
            }
        }

        // Step 5: map condensed ranks back to individual nodes
        final Map<String, Integer> rank = new HashMap<>();
        for (final String n : allNodes) {
            final int scc = nodeToScc.getOrDefault(n, -1);
            rank.put(n, scc >= 0 ? sccRank.getOrDefault(scc, 0) : 0);
        }
        return rank;
    }

    // -------------------------------------------------------------------------
    // Tarjan's SCC — iterates allNodes sorted for determinism
    // -------------------------------------------------------------------------

    private List<List<String>> findSCCs(
            final Map<String, Set<String>> graph,
            final Set<String> allNodes) {
        final Map<String, Integer> disc = new HashMap<>();
        final Map<String, Integer> low = new HashMap<>();
        final Set<String> onStack = new HashSet<>();
        final ArrayDeque<String> stack = new ArrayDeque<>();
        final List<List<String>> sccs = new ArrayList<>();
        final int[] time = {0};

        for (final String n : allNodes.stream().sorted().collect(Collectors.toList())) {
            if (!disc.containsKey(n)) {
                sccDFS(n, graph, disc, low, onStack, stack, sccs, time);
            }
        }
        return sccs;
    }

    private void sccDFS(
            final String u,
            final Map<String, Set<String>> graph,
            final Map<String, Integer> disc,
            final Map<String, Integer> low,
            final Set<String> onStack,
            final ArrayDeque<String> stack,
            final List<List<String>> sccs,
            final int[] time) {
        disc.put(u, time[0]);
        low.put(u, time[0]);
        time[0]++;
        stack.push(u);
        onStack.add(u);

        for (final String v : graph.getOrDefault(u, Set.of())) {
            if (!disc.containsKey(v)) {
                sccDFS(v, graph, disc, low, onStack, stack, sccs, time);
                low.put(u, Math.min(low.get(u), low.get(v)));
            } else if (onStack.contains(v)) {
                low.put(u, Math.min(low.get(u), disc.get(v)));
            }
        }

        if (low.get(u).equals(disc.get(u))) {
            final List<String> scc = new ArrayList<>();
            String w;
            do {
                w = stack.pop();
                onStack.remove(w);
                scc.add(w);
            } while (!w.equals(u));
            sccs.add(scc);
        }
    }

    // -------------------------------------------------------------------------
    // Reachability — BFS, cycle-safe via visited set
    // -------------------------------------------------------------------------

    private boolean isReachable(
            final String from,
            final String to,
            final Map<String, Set<String>> graph) {
        if (from.equals(to)) {
            return false;
        }
        final Set<String> visited = new HashSet<>();
        final ArrayDeque<String> queue = new ArrayDeque<>();
        queue.add(from);
        while (!queue.isEmpty()) {
            final String cur = queue.poll();
            if (!visited.add(cur)) {
                continue;
            }
            for (final String next : graph.getOrDefault(cur, Set.of())) {
                if (to.equals(next)) {
                    return true;
                }
                if (!visited.contains(next)) {
                    queue.add(next);
                }
            }
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // Fix 1: WCC helpers — BFS on the undirected view of the clean graph
    // -------------------------------------------------------------------------

    private Map<String, Set<String>> buildUndirectedGraph(
            final Map<String, Set<String>> directed,
            final Set<String> allNodes) {
        final Map<String, Set<String>> undirected = new HashMap<>();
        for (final String n : allNodes) {
            undirected.put(n, new HashSet<>());
        }
        for (final Map.Entry<String, Set<String>> e : directed.entrySet()) {
            for (final String dst : e.getValue()) {
                undirected.computeIfAbsent(e.getKey(), k -> new HashSet<>()).add(dst);
                undirected.computeIfAbsent(dst, k -> new HashSet<>()).add(e.getKey());
            }
        }
        return undirected;
    }

    private boolean sameWeaklyConnectedComponent(
            final String a,
            final String b,
            final Map<String, Set<String>> undirected) {
        if (a.equals(b)) {
            return true;
        }
        final Set<String> visited = new HashSet<>();
        final ArrayDeque<String> queue = new ArrayDeque<>();
        queue.add(a);
        while (!queue.isEmpty()) {
            final String cur = queue.poll();
            if (!visited.add(cur)) {
                continue;
            }
            for (final String next : undirected.getOrDefault(cur, Set.of())) {
                if (b.equals(next)) {
                    return true;
                }
                if (!visited.contains(next)) {
                    queue.add(next);
                }
            }
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // Source grouping
    // -------------------------------------------------------------------------

    /**
     * Groups sources into structural-sibling clusters for the split phase.
     *
     * <p>Two sources belong to the <em>same</em> group iff they are:
     * <ul>
     *   <li>mutually unreachable in the clean graph, <em>and</em>
     *   <li>at the same pipeline rank (same depth from any root), <em>and</em>
     *   <li>in the same weakly-connected component of the clean graph (Fix 1 — disconnected
     *       roots with no shared branch ancestry must not be grouped together).
     * </ul>
     * Sources are processed rank-ascending then ID-ascending for determinism.
     */
    private List<List<String>> groupSources(
            final List<String> sources,
            final Map<String, Set<String>> graph,
            final Map<String, Integer> ranks,
            final Set<String> allNodes) {

        // Build undirected graph once per logger; used for the WCC check below
        final Map<String, Set<String>> undirected = buildUndirectedGraph(graph, allNodes);

        final List<String> sorted = sources.stream()
                .sorted(Comparator
                        .comparingInt((String s) -> ranks.getOrDefault(s, 0))
                        .thenComparing(Comparator.naturalOrder()))
                .collect(Collectors.toList());

        final List<List<String>> groups = new ArrayList<>();
        for (final String src : sorted) {
            boolean placed = false;
            for (final List<String> group : groups) {
                if (isStructuralSiblingWithAll(src, group, graph, ranks, undirected)) {
                    group.add(src);
                    placed = true;
                    break;
                }
            }
            if (!placed) {
                final List<String> newGroup = new ArrayList<>();
                newGroup.add(src);
                groups.add(newGroup);
            }
        }
        return groups;
    }

    /**
     * Used in the <em>split</em> phase: requires mutual unreachability, same rank, AND same WCC.
     */
    private boolean isStructuralSiblingWithAll(
            final String src,
            final List<String> group,
            final Map<String, Set<String>> graph,
            final Map<String, Integer> ranks,
            final Map<String, Set<String>> undirected) {
        for (final String member : group) {
            if (!isParallel(src, member, graph, ranks)) {
                return false;
            }
            if (!sameWeaklyConnectedComponent(src, member, undirected)) {
                return false;
            }
            if (!hasCommonAncestor(src, member, graph)) {
                return false;
            }
        }
        return true;
    }

    private boolean hasCommonAncestor(
            final String first,
            final String second,
            final Map<String, Set<String>> graph) {
        final Map<String, Set<String>> incoming = new HashMap<>();
        for (final Map.Entry<String, Set<String>> entry : graph.entrySet()) {
            for (final String destination : entry.getValue()) {
                incoming.computeIfAbsent(destination, ignored -> new HashSet<>())
                        .add(entry.getKey());
            }
        }
        final Set<String> firstAncestors = ancestorsOf(first, incoming);
        final Set<String> secondAncestors = ancestorsOf(second, incoming);
        firstAncestors.retainAll(secondAncestors);
        return !firstAncestors.isEmpty();
    }

    private Set<String> ancestorsOf(
            final String source,
            final Map<String, Set<String>> incoming) {
        final Set<String> ancestors = new HashSet<>();
        final ArrayDeque<String> pending = new ArrayDeque<>(
                incoming.getOrDefault(source, Set.of()));
        while (!pending.isEmpty()) {
            final String current = pending.removeFirst();
            if (ancestors.add(current)) {
                pending.addAll(incoming.getOrDefault(current, Set.of()));
            }
        }
        return ancestors;
    }

    private boolean isParallel(
            final String a,
            final String b,
            final Map<String, Set<String>> graph,
            final Map<String, Integer> ranks) {
        return !isReachable(a, b, graph)
                && !isReachable(b, a, graph)
                && Objects.equals(ranks.get(a), ranks.get(b));
    }

    // -------------------------------------------------------------------------
    // Phase 1: Split  (Fix 5: capture base name before any rename)
    // -------------------------------------------------------------------------

    private void splitLoggers(
            final List<Map<String, Object>> processors,
            final Map<String, Map<String, Object>> byId,
            final Set<String> loggerIds,
            final List<Map<String, Object>> connections,
            final Map<String, Set<String>> graph,
            final Map<String, Integer> ranks,
            final Set<String> generatedIds,
            final Set<String> splitProtectedIds) {

        // usedIds tracks all IDs seen so far to avoid collisions when generating clone IDs
        final Set<String> usedIds = new HashSet<>(generatedIds);

        // Fix 8: process loggers in sorted order for deterministic clone-ID assignment
        for (final String loggerId : List.copyOf(loggerIds).stream().sorted().collect(Collectors.toList())) {
            if (!generatedIds.contains(loggerId)) {
                continue; // canvas logger — do not touch
            }
            final Map<String, Object> loggerProc = byId.get(loggerId);
            if (loggerProc == null) {
                continue;
            }
            if (Boolean.TRUE.equals(loggerProc.get("preserve_separate_terminal"))) {
                continue; // explicitly kept separate by the model
            }
            final boolean hasAllowedTerminalOutput = connections.stream()
                    .filter(connection ->
                            loggerId.equals(String.valueOf(connection.get("from"))))
                    .anyMatch(connection ->
                            Boolean.TRUE.equals(connection.get("allow_terminal_output")));
            if (hasAllowedTerminalOutput) {
                continue;
            }

            final List<String> sources =
                    directGeneratedSources(loggerId, loggerIds, connections, generatedIds);
            if (sources.size() < 2) {
                continue; // nothing to split
            }

            // Fix 1 + Fix 2: pass allNodes for WCC check and SCC-accurate ranks
            final List<List<String>> groups = groupSources(sources, graph, ranks, generatedIds);
            if (groups.size() < 2) {
                continue; // all sources are structural siblings — keep shared
            }

            log.warn("Splitting terminal logger {} into {} source groups", loggerId, groups.size());

            // Mark the original and every clone as split-protected so consolidation cannot
            // re-merge loggers that were separated for structural/WCC reasons.
            splitProtectedIds.add(loggerId);
            loggerProc.put("preserve_separate_terminal", true);

            // Fix 5: capture the immutable original base name BEFORE any renaming so that every
            // group name is derived from the same base and names do not accumulate.
            final String originalBaseName = String.valueOf(
                    loggerProc.getOrDefault("name", loggerProc.getOrDefault("id", "Log")));

            final List<String> firstGroupSorted = toSortedList(groups.get(0));
            loggerProc.put("name", buildGroupName(originalBaseName, firstGroupSorted, byId));

            // Create clones for the remaining groups and redirect their connections
            for (int i = 1; i < groups.size(); i++) {
                final List<String> groupSorted = toSortedList(groups.get(i));
                final String cloneId = makeCloneId(loggerId, groupSorted, usedIds);

                final Map<String, Object> clone = new LinkedHashMap<>(loggerProc);
                clone.put("id", cloneId);
                // Fix 5: derive clone name from originalBaseName, not from the already-renamed loggerProc
                clone.put("name", buildGroupName(originalBaseName, groupSorted, byId));

                processors.add(clone);
                byId.put(cloneId, clone);
                loggerIds.add(cloneId);
                generatedIds.add(cloneId);
                splitProtectedIds.add(cloneId); // protect clone from consolidation too

                final Set<String> groupSourceSet = new HashSet<>(groups.get(i));
                for (final Map<String, Object> conn : connections) {
                    if (groupSourceSet.contains(String.valueOf(conn.get("from")))
                            && loggerId.equals(String.valueOf(conn.get("to")))) {
                        conn.put("to", cloneId);
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Phase 2: Consolidate  (Fix 3, 4, 6, 7)
    // -------------------------------------------------------------------------

    private void consolidateLoggers(
            final List<Map<String, Object>> processors,
            final Map<String, Map<String, Object>> byId,
            final Set<String> loggerIds,
            final List<Map<String, Object>> connections,
            final Map<String, Set<String>> graph,
            final Map<String, Integer> ranks,
            final Set<String> generatedIds,
            final Set<String> splitProtectedIds) {

        final Map<String, Outcome> outcomes = new HashMap<>();
        final Map<String, List<String>> sourcesByLogger = new HashMap<>();

        for (final String loggerId : List.copyOf(loggerIds)) {
            if (!byId.containsKey(loggerId)) {
                continue;
            }
            final Map<String, Object> proc = byId.get(loggerId);
            if (Boolean.TRUE.equals(proc.get("preserve_separate_terminal"))) {
                continue;
            }
            // Fix 3: pass generatedIds/loggerIds so invalid connections are filtered out
            outcomes.put(loggerId, detectOutcome(loggerId, connections, loggerIds, generatedIds));
            sourcesByLogger.put(loggerId,
                    directGeneratedSources(loggerId, loggerIds, connections, generatedIds));
        }

        // Fix 4: group by (outcome, normalizedType, behaviorKey) — not just outcome.
        // LogAttribute ≠ LogMessage; differing configs must not be merged.
        final Map<String, List<String>> byKey = new LinkedHashMap<>();
        for (final Map.Entry<String, Outcome> e : outcomes.entrySet()) {
            if (e.getValue() == Outcome.MIXED) {
                continue;
            }
            final Map<String, Object> proc = byId.get(e.getKey());
            if (proc == null) {
                continue;
            }
            final String key = e.getValue().name()
                    + "|" + normalizeType(String.valueOf(proc.getOrDefault("type", "")))
                    + "|" + behaviorKey(proc);
            byKey.computeIfAbsent(key, k -> new ArrayList<>()).add(e.getKey());
        }

        for (final List<String> group : byKey.values()) {
            if (group.size() < 2) {
                continue; // only one logger for this equivalence class — nothing to consolidate
            }

            // Never re-merge loggers that were separated by a structural split
            if (group.stream().anyMatch(splitProtectedIds::contains)) {
                continue;
            }

            final List<String> allSources = group.stream()
                    .flatMap(id -> sourcesByLogger.getOrDefault(id, List.of()).stream())
                    .distinct()
                    .collect(Collectors.toList());

            if (!allStructuralSiblings(allSources, graph, ranks, generatedIds)) {
                continue; // sources are not all parallel — do not consolidate
            }

            // Fix 8: deterministic keep selection — alphabetically first ID
            group.sort(Comparator.naturalOrder());
            final String keepId = group.get(0);

            for (int i = 1; i < group.size(); i++) {
                final String removeId = group.get(i);

                // Fix 6: if an external/canvas source targets this logger, skip its removal
                // entirely to prevent dangling or silently rewritten external references.
                final boolean hasExternalIncoming = connections.stream()
                        .filter(c -> removeId.equals(String.valueOf(c.get("to"))))
                        .anyMatch(c -> {
                            final String from = String.valueOf(c.get("from"));
                            return !removeId.equals(from) && !generatedIds.contains(from);
                        });
                if (hasExternalIncoming) {
                    log.warn("Skipping consolidation of {} — has external incoming connections", removeId);
                    continue;
                }

                final boolean hasAllowedTerminalOutput = connections.stream()
                        .filter(c -> removeId.equals(String.valueOf(c.get("from"))))
                        .anyMatch(c -> Boolean.TRUE.equals(c.get("allow_terminal_output")));
                if (hasAllowedTerminalOutput) {
                    log.warn("Skipping consolidation of {} — has allowed terminal output", removeId);
                    continue;
                }

                // Fix 7: remove all outgoing connections from the removed logger before deleting
                // it so no dangling source reference survives past the final LlmClient filter.
                connections.removeIf(c -> removeId.equals(String.valueOf(c.get("from"))));

                // Fix 6: redirect only generated incoming connections (never rewrite external refs)
                for (final Map<String, Object> conn : connections) {
                    final String from = String.valueOf(conn.get("from"));
                    if (removeId.equals(String.valueOf(conn.get("to")))
                            && generatedIds.contains(from)) {
                        conn.put("to", keepId);
                    }
                }

                processors.removeIf(p -> removeId.equals(String.valueOf(p.get("id"))));
                byId.remove(removeId);
                loggerIds.remove(removeId);
                generatedIds.remove(removeId);
                log.warn("Consolidated parallel terminal logger {} into {}", removeId, keepId);
            }
            // Fix 4: preserve the kept logger's existing name — do NOT overwrite it.
        }
    }

    // -------------------------------------------------------------------------
    // Fix 3: Outcome detection — ignores self-loops, non-generated, and terminal-source edges
    // -------------------------------------------------------------------------

    private Outcome detectOutcome(
            final String loggerId,
            final List<Map<String, Object>> connections,
            final Set<String> loggerIds,
            final Set<String> generatedIds) {
        boolean success = false;
        boolean failure = false;
        for (final Map<String, Object> conn : connections) {
            if (!loggerId.equals(String.valueOf(conn.get("to")))) {
                continue;
            }
            final String from = String.valueOf(conn.get("from"));
            // Skip self-loops — an invalid self-loop must not skew outcome classification
            if (loggerId.equals(from)) {
                continue;
            }
            // Skip non-generated (canvas-only) sources
            if (!generatedIds.contains(from)) {
                continue;
            }
            // Skip sources that are themselves terminal loggers
            if (loggerIds.contains(from)) {
                continue;
            }
            final Object rels = conn.get("relationships");
            if (!(rels instanceof List<?>)) {
                continue;
            }
            for (final Object r : (List<?>) rels) {
                final String rel = String.valueOf(r).toLowerCase();
                if (SUCCESS_RELS.contains(rel)) {
                    success = true;
                }
                if (FAILURE_RELS.contains(rel)) {
                    failure = true;
                }
            }
        }
        if (success && !failure) {
            return Outcome.SUCCESS;
        }
        if (failure && !success) {
            return Outcome.FAILURE;
        }
        return Outcome.MIXED;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Returns the direct generated (non-logger) sources of the given terminal logger.
     * Only sources present in the generated processor list are returned; canvas references
     * are excluded so they are never reclassified or redirected.
     * Fix 8: result is sorted for connection-order-independent determinism.
     */
    private List<String> directGeneratedSources(
            final String loggerId,
            final Set<String> loggerIds,
            final List<Map<String, Object>> connections,
            final Set<String> generatedIds) {
        return connections.stream()
                .filter(c -> loggerId.equals(String.valueOf(c.get("to"))))
                .filter(c -> !loggerId.equals(String.valueOf(c.get("from"))))
                .filter(c -> !loggerIds.contains(String.valueOf(c.get("from"))))
                .filter(c -> generatedIds.contains(String.valueOf(c.get("from"))))
                .map(c -> String.valueOf(c.get("from")))
                .distinct()
                .sorted() // Fix 8: deterministic regardless of connection-list order
                .collect(Collectors.toList());
    }

    private boolean allStructuralSiblings(
            final List<String> sources,
            final Map<String, Set<String>> graph,
            final Map<String, Integer> ranks,
            final Set<String> allNodes) {
        final Map<String, Set<String>> undirected = buildUndirectedGraph(graph, allNodes);
        for (int i = 0; i < sources.size(); i++) {
            for (int j = i + 1; j < sources.size(); j++) {
                if (!isParallel(sources.get(i), sources.get(j), graph, ranks)
                        || !sameWeaklyConnectedComponent(
                                sources.get(i), sources.get(j), undirected)
                        || !hasCommonAncestor(
                                sources.get(i), sources.get(j), graph)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Derives a human-readable name for a partitioned logger clone.
     * The first (alphabetically by ID) source name is appended to the supplied base name.
     * Fix 5: callers pass the immutable {@code baseName} captured before any rename.
     */
    private String buildGroupName(
            final String baseName,
            final List<String> sortedGroupSources,
            final Map<String, Map<String, Object>> byId) {
        if (sortedGroupSources.isEmpty()) {
            return baseName;
        }
        final String firstId = sortedGroupSources.get(0);
        final Map<String, Object> firstProc = byId.get(firstId);
        final String firstName = firstProc != null
                ? String.valueOf(firstProc.getOrDefault("name", firstId))
                : firstId;
        return baseName + " - " + firstName;
    }

    /**
     * Creates a stable, deterministic clone ID derived from the logger ID and the sorted source
     * group.  Uses the first (sorted) source ID as the distinguishing key, appending a numeric
     * suffix on collision.
     */
    private String makeCloneId(
            final String loggerId,
            final List<String> sortedGroupSources,
            final Set<String> usedIds) {
        final String key = sortedGroupSources.isEmpty() ? "grp" : sortedGroupSources.get(0);
        String candidate = loggerId + "-for-" + key;
        int suffix = 2;
        while (!usedIds.add(candidate)) {
            candidate = loggerId + "-for-" + key + "-" + suffix++;
        }
        return candidate;
    }

    private List<String> toSortedList(final List<String> ids) {
        return ids.stream().sorted().collect(Collectors.toList());
    }

    // Fix 4: type and behavior-key helpers for consolidation equivalence

    /** Strips the package prefix and lowercases so "LogAttribute" ≡ "org...LogAttribute". */
    private String normalizeType(final String type) {
        if (type == null || type.isEmpty()) {
            return "";
        }
        final String lower = type.toLowerCase();
        final int dot = lower.lastIndexOf('.');
        return dot >= 0 ? lower.substring(dot + 1) : lower;
    }

    /**
     * Produces a canonical string of all behavior fields (everything except identity/presentation
     * /control fields) so that two processors with the same configuration compare equal.
     */
    private String behaviorKey(final Map<String, Object> proc) {
        return proc.entrySet().stream()
                .filter(e -> !IDENTITY_FIELDS.contains(e.getKey()))
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getKey() + "=" + sortedString(e.getValue()))
                .collect(Collectors.joining(","));
    }

    /** Recursively serializes an object in a key-sorted, deterministic fashion. */
    private String sortedString(final Object obj) {
        if (obj instanceof Map<?, ?> m) {
            return m.entrySet().stream()
                    .sorted(Comparator.comparing(e -> String.valueOf(e.getKey())))
                    .map(e -> String.valueOf(e.getKey()) + "=" + sortedString(e.getValue()))
                    .collect(Collectors.joining(",", "{", "}"));
        }
        if (obj instanceof List<?> l) {
            return l.stream()
                    .map(this::sortedString)
                    .collect(Collectors.joining(",", "[", "]"));
        }
        return String.valueOf(obj);
    }
}
