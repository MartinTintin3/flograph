import { useSetSettings, useSigma } from "@react-sigma/core";
import { Attributes } from "graphology-types";
import { FC, PropsWithChildren, useEffect } from "react";

import { drawHover, drawLabel } from "../canvas-utils";
import useDebounce from "../use-debounce";

const NODE_FADE_COLOR = "#bbb";
const EDGE_FADE_COLOR = "#eee";

const GraphSettingsController: FC<PropsWithChildren<{ hoveredNode: string | null; focusedNode: string | null }>> = ({ children, hoveredNode, focusedNode }) => {
  const sigma = useSigma();
  const setSettings = useSetSettings();
  const graph = sigma.getGraph();

  // Here we debounce the value to avoid having too much highlights refresh when
  // moving the mouse over the graph:
  const debouncedHoveredNode = useDebounce(hoveredNode, 40);

  /**
   * Initialize here settings that require to know the graph and/or the sigma
   * instance:
   */
  useEffect(() => {
    const hoveredColor: string = (debouncedHoveredNode && sigma.getNodeDisplayData(debouncedHoveredNode)?.color) || "";

    setSettings({
      defaultDrawNodeLabel: drawLabel,
      defaultDrawNodeHover: drawHover,
      nodeReducer: (node: string, data: Attributes) => {
        if (debouncedHoveredNode) {
          const isConnected = node === debouncedHoveredNode ||
            graph.hasEdge(node, debouncedHoveredNode) ||
            graph.hasEdge(debouncedHoveredNode, node);

          return isConnected
            ? { ...data, zIndex: 1, forceLabel: true }
            : { ...data, zIndex: 0, label: "", color: NODE_FADE_COLOR, image: null, highlighted: false };
        }
        return data;
      },
      edgeReducer: (edge: string, data: Attributes) => {
        if (debouncedHoveredNode) {
          return graph.hasExtremity(edge, debouncedHoveredNode)
            ? { ...data, color: hoveredColor, size: 4 }
            : { ...data, color: EDGE_FADE_COLOR, hidden: true };
        }
        return data;
      },
    });
  }, [sigma, graph, debouncedHoveredNode]);

  /**
   * Update node and edge reducers when a node is hovered, to highlight its
   * neighborhood:
   */
  useEffect(() => {
    const hoveredColor: string = (debouncedHoveredNode && sigma.getNodeDisplayData(debouncedHoveredNode)?.color) || "";

    sigma.setSetting(
      "nodeReducer",
      debouncedHoveredNode
        ? (node, data) => {
            const isConnected = node === debouncedHoveredNode ||
              graph.hasEdge(node, debouncedHoveredNode) ||
              graph.hasEdge(debouncedHoveredNode, node);

            return isConnected
              ? { ...data, zIndex: 1, forceLabel: true }
              : { ...data, zIndex: 0, label: "", color: NODE_FADE_COLOR, image: null, highlighted: false };
          }
        : null,
    );
    sigma.setSetting(
      "edgeReducer",
      debouncedHoveredNode
        ? (edge, data) =>
            graph.hasExtremity(edge, debouncedHoveredNode)
              ? { ...data, color: hoveredColor, size: 4 }
              : { ...data, color: EDGE_FADE_COLOR, hidden: true }
        : null,
    );
  }, [debouncedHoveredNode]);

  /**
   * Update node and edge reducers when a node is focused (clicked), to show all
   * connected node labels regardless of size threshold:
   */
  useEffect(() => {
    if (focusedNode) {
      const focusedColor: string = sigma.getNodeDisplayData(focusedNode)?.color || "";

      sigma.setSetting(
        "nodeReducer",
        (node, data) => {
          const isConnected =
            node === focusedNode ||
            graph.hasEdge(node, focusedNode) ||
            graph.hasEdge(focusedNode, node);

          if (isConnected) {
            // Force label to show for focused node and its neighbors
            return { ...data, zIndex: 1, forceLabel: true };
          } else {
            // Fade out all other nodes
            return { ...data, zIndex: 0, label: "", color: NODE_FADE_COLOR, image: null, highlighted: false };
          }
        },
      );

      sigma.setSetting(
        "edgeReducer",
        (edge, data) =>
          graph.hasExtremity(edge, focusedNode)
            ? { ...data, color: focusedColor, size: 4 }
            : { ...data, color: EDGE_FADE_COLOR, hidden: true },
      );
    } else {
      // Reset reducers when no node is focused
      sigma.setSetting("nodeReducer", null);
      sigma.setSetting("edgeReducer", null);
    }
  }, [focusedNode]);

  return <>{children}</>;
};

export default GraphSettingsController;
