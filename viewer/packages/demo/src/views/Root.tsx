import { FullScreenControl, SigmaContainer, ZoomControl } from "@react-sigma/core";
import { DirectedGraph } from "graphology";
import { FC, useEffect, useMemo, useState } from "react";
import { BiBookContent, BiRadioCircleMarked } from "react-icons/bi";
import { BsArrowsFullscreen, BsFullscreenExit, BsZoomIn, BsZoomOut } from "react-icons/bs";
import { GrClose } from "react-icons/gr";
import { Settings } from "sigma/settings";

import { drawHover, drawLabel } from "../canvas-utils";
import { GraphDataset } from "../types";
import DescriptionPanel from "./DescriptionPanel";
import GraphEventsController from "./GraphEventsController";
import GraphSettingsController from "./GraphSettingsController";
import GraphTitle from "./GraphTitle";
import NodeDetailsPanel from "./NodeDetailsPanel";
import SearchField from "./SearchField";
import SpacingControl from "./LabelControl";

const GRAPH_PATH = "./graph.json";
const SAMPLE_GRAPH_PATH = "./graph.sample.json";

const Root: FC = () => {
  const graph = useMemo(() => new DirectedGraph(), []);
  const [showContents, setShowContents] = useState(false);
  const [dataReady, setDataReady] = useState(false);
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);
  const [focusedNode, setFocusedNode] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [labelSizeThreshold, setLabelSizeThreshold] = useState<number>(10);
  const sigmaSettings: Partial<Settings> = useMemo(
    () => ({
      defaultDrawNodeLabel: drawLabel,
      defaultDrawNodeHover: drawHover,
      defaultNodeType: "circle",
      defaultEdgeType: "arrow",
      labelDensity: 0.5,
      labelGridCellSize: 30,
      labelRenderedSizeThreshold: labelSizeThreshold,
      labelAllowOverlap: true,
      labelFont: "Lato, sans-serif",
      zIndex: true,
    }),
    [labelSizeThreshold]
  );

  useEffect(() => {
    let cancelled = false;

    const hydrateGraph = (dataset: GraphDataset) => {
      graph.clear();
      dataset.nodes.forEach(({ id, attributes }) => {
        graph.addNode(id, {
          ...attributes,
          baseX: attributes.x,
          baseY: attributes.y,
          label: attributes.label || id,
        });
      });
      dataset.edges.forEach((edge) => {
        if (!graph.hasNode(edge.source) || !graph.hasNode(edge.target)) return;
        if (graph.hasEdge(edge.key)) return;
        graph.addEdgeWithKey(edge.key, edge.source, edge.target, {
          size: 1,
          ...(edge.attributes || {}),
        });
      });
      if (!cancelled) {
        setFocusedNode(null);
        setHoveredNode(null);
        requestAnimationFrame(() => setDataReady(true));
      }
    };

    const fetchGraph = async (path: string) => {
      const response = await fetch(path, { cache: "no-store" });
      if (!response.ok) {
        throw new Error(`Unable to load ${path}`);
      }
      return (await response.json()) as GraphDataset;
    };

    const loadData = async () => {
      setDataReady(false);
      setErrorMessage(null);
      try {
        const data = await fetchGraph(GRAPH_PATH);
        hydrateGraph(data);
        setStatusMessage(null);
      } catch (primaryError) {
        try {
          const fallback = await fetchGraph(SAMPLE_GRAPH_PATH);
          hydrateGraph(fallback);
          setStatusMessage("graph.json not found – displaying bundled sample graph.");
        } catch (secondaryError) {
          if (!cancelled) {
            setErrorMessage(
              "Could not load graph.json. Run export.py and copy the output into packages/demo/public/graph.json.",
            );
          }
        }
      }
    };

    loadData();
    window['graph'] = graph;  

    return () => {
      cancelled = true;
    };

  }, [graph]);

  return (
    <div id="app-root" className={showContents ? "show-contents" : ""}>
      <SigmaContainer graph={graph} settings={sigmaSettings} className="react-sigma">
        <GraphSettingsController hoveredNode={hoveredNode} focusedNode={focusedNode} />
        <GraphEventsController setHoveredNode={setHoveredNode} setFocusedNode={setFocusedNode} />

        {!dataReady && !errorMessage && <div className="loading-banner">Loading graph data…</div>}
        {errorMessage && <div className="loading-banner error">{errorMessage}</div>}

        {dataReady && !errorMessage && (
          <>
            <div className="controls">
              <div className="react-sigma-control ico">
                <button
                  type="button"
                  className="show-contents"
                  onClick={() => setShowContents(true)}
                  title="Show caption and description"
                >
                  <BiBookContent />
                </button>
              </div>
              <FullScreenControl className="ico">
                <BsArrowsFullscreen />
                <BsFullscreenExit />
              </FullScreenControl>

              <ZoomControl className="ico">
                <BsZoomIn />
                <BsZoomOut />
                <BiRadioCircleMarked />
              </ZoomControl>
            </div>
            <div className="contents">
              <div className="ico">
                <button
                  type="button"
                  className="ico hide-contents"
                  onClick={() => setShowContents(false)}
                  title="Show caption and description"
                >
                  <GrClose />
                </button>
              </div>
              <GraphTitle subtitle={statusMessage || undefined} />
              <div className="panels">
                <SearchField onSelectNode={setFocusedNode} />
                <SpacingControl min={0} max={Math.max(...graph.mapNodes((_, n) => n.size))} value={labelSizeThreshold} onChange={setLabelSizeThreshold} />
                <DescriptionPanel />
                <NodeDetailsPanel graph={graph} nodeId={focusedNode || hoveredNode} />
              </div>
            </div>
          </>
        )}
      </SigmaContainer>
    </div>
  );
};

export default Root;
