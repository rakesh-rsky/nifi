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

package in.shrake.nifi.layout.core.layout;

import in.shrake.nifi.layout.core.model.LayoutNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CrossingCountComputer {

    public static int countCrossings(List<List<LayoutNode>> layers, Map<String, List<String>> downNeighbors) {
        int crossings = 0;
        for (int l = 0; l < layers.size() - 1; l++) {
            List<LayoutNode> topLayer = layers.get(l);
            List<LayoutNode> bottomLayer = layers.get(l+1);
            
            Map<String, Integer> bottomPos = new LinkedHashMap<>();
            for (int i = 0; i < bottomLayer.size(); i++) {
                bottomPos.put(bottomLayer.get(i).getId(), i);
            }
            
            List<Integer> targetPositions = new ArrayList<>();
            for (LayoutNode topNode : topLayer) {
                List<String> targets = downNeighbors.getOrDefault(topNode.getId(), new ArrayList<>());
                List<Integer> posList = new ArrayList<>();
                for (String t : targets) {
                    Integer p = bottomPos.get(t);
                    if (p != null) {
                        posList.add(p);
                    }
                }
                posList.sort(Integer::compareTo);
                targetPositions.addAll(posList);
            }
            
            crossings += countInversions(targetPositions.toArray(new Integer[0]));
        }
        return crossings;
    }

    private static int countInversions(Integer[] arr) {
        if (arr.length <= 1) return 0;
        Integer[] temp = new Integer[arr.length];
        return mergeSortAndCount(arr, temp, 0, arr.length - 1);
    }

    private static int mergeSortAndCount(Integer[] arr, Integer[] temp, int left, int right) {
        int invCount = 0;
        if (left < right) {
            int mid = (left + right) / 2;
            invCount += mergeSortAndCount(arr, temp, left, mid);
            invCount += mergeSortAndCount(arr, temp, mid + 1, right);
            invCount += merge(arr, temp, left, mid, right);
        }
        return invCount;
    }

    private static int merge(Integer[] arr, Integer[] temp, int left, int mid, int right) {
        int i = left;
        int j = mid + 1;
        int k = left;
        int invCount = 0;

        while ((i <= mid) && (j <= right)) {
            if (arr[i] <= arr[j]) {
                temp[k++] = arr[i++];
            } else {
                temp[k++] = arr[j++];
                invCount += (mid + 1 - i);
            }
        }

        while (i <= mid) {
            temp[k++] = arr[i++];
        }

        while (j <= right) {
            temp[k++] = arr[j++];
        }

        for (i = left; i <= right; i++) {
            arr[i] = temp[i];
        }

        return invCount;
    }
}
