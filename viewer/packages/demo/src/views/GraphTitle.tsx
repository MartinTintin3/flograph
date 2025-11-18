import { useSigma } from "@react-sigma/core";
import { FC } from "react";

const GraphTitle: FC<{ subtitle?: string }> = ({ subtitle }) => {
  const sigma = useSigma();
  const graph = sigma.getGraph();

  return (
    <div className="graph-title">
      <h1>Wrestling matchup network explorer</h1>
      <h2>
        <i>
          Rendering {graph.order} wrestler{graph.order === 1 ? "" : "s"} and {graph.size} directed matchups.
          {subtitle ? ` ${subtitle}` : ""}
        </i>
      </h2>
    </div>
  );
};

export default GraphTitle;
