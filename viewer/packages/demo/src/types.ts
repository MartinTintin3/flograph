export interface NodeAttributes {
  label: string;
  x: number;
  y: number;
  color: string;
  size: number;
  wins: number;
  losses: number;
}

export interface EdgeAttributes {
  type?: string;
}

export interface GraphNode {
  id: string;
  attributes: NodeAttributes;
}

export interface GraphEdge {
  key: string;
  source: string;
  target: string;
  attributes?: EdgeAttributes;
}

export interface GraphDataset {
  nodes: GraphNode[];
  edges: GraphEdge[];
}
