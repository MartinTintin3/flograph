import { useRegisterEvents, useSigma } from "@react-sigma/core";
import { FC, PropsWithChildren, useEffect } from "react";

function getMouseLayer() {
  return document.querySelector(".sigma-mouse");
}

const GraphEventsController: FC<
  PropsWithChildren<{
    setHoveredNode: (node: string | null) => void;
    setFocusedNode: (node: string | null) => void;
  }>
> = ({ setHoveredNode, setFocusedNode, children }) => {
  const sigma = useSigma();
  const graph = sigma.getGraph();
  const registerEvents = useRegisterEvents();

  /**
   * Initialize here settings that require to know the graph and/or the sigma
   * instance:
   */
  useEffect(() => {
    registerEvents({
      clickNode({ node }) {
        if (!graph.getNodeAttribute(node, "hidden")) {
          setFocusedNode(node);
          const nodeDisplayData = sigma.getNodeDisplayData(node);
          if (nodeDisplayData)
            sigma.getCamera().animate(
              { ...nodeDisplayData, ratio: 0.05 },
              {
                duration: 600,
              },
            );
        }
      },
      enterNode({ node }) {
        setHoveredNode(node);
        // TODO: Find a better way to get the DOM mouse layer:
        const mouseLayer = getMouseLayer();
        if (mouseLayer) mouseLayer.classList.add("mouse-pointer");
      },
      leaveNode() {
        setHoveredNode(null);
        // TODO: Find a better way to get the DOM mouse layer:
        const mouseLayer = getMouseLayer();
        if (mouseLayer) mouseLayer.classList.remove("mouse-pointer");
      },
      clickStage() {
        setFocusedNode(null);
        setHoveredNode(null);
      },
    });
  }, []);

  return <>{children}</>;
};

export default GraphEventsController;
