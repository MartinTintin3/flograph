import { DirectedGraph } from "graphology";
import { FC } from "react";
import { BsInfoCircle } from "react-icons/bs";

import Panel from "./Panel";

interface NodeDetailsPanelProps {
  graph: DirectedGraph;
  nodeId: string | null;
}

const NodeDetailsPanel: FC<NodeDetailsPanelProps> = ({ graph, nodeId }) => {
  const hasNode = !!nodeId && graph.hasNode(nodeId);
  const attributes = hasNode ? graph.getNodeAttributes(nodeId as string) : null;
  const wins = attributes?.wins ?? 0;
  const losses = attributes?.losses ?? 0;
  const matches = wins + losses;

  return (
    <Panel
      initiallyDeployed
      title={
        <>
          <BsInfoCircle className="text-muted" /> Wrestler details
        </>
      }
    >
      {!hasNode && (
        <p className="text-muted">
          Hover a node—or pick one from the search box—to see its record and layout coordinates.
        </p>
      )}
      {hasNode && attributes && (
        <ul className="details-list">
          <li>
            <strong>{attributes.label}</strong>
          </li>
          <li>
            Matches: <strong>{matches}</strong>
          </li>
          <li>
            Wins / Losses: <strong>{wins}</strong> / <strong>{losses}</strong>
          </li>
          <li>
            Win %: <strong>{matches ? ((wins / matches) * 100).toFixed(1) : "0.0"}%</strong>
          </li>
          <li>
            Layout coords: (<code>{attributes.x.toFixed(2)}</code>, <code>{attributes.y.toFixed(2)}</code>)
          </li>
        </ul>
      )}
    </Panel>
  );
};

export default NodeDetailsPanel;
