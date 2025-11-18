import { ChangeEvent, FC } from "react";
import { BiMove } from "react-icons/bi";

import Panel from "./Panel";

const MIN_SCALE = 0.5;
const MAX_SCALE = 3;
const STEP = 0.1;

interface SpacingControlProps {
  value: number;
  onChange: (value: number) => void;
}

const SpacingControl: FC<SpacingControlProps> = ({ value, onChange }) => {
  const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange(Number(event.target.value));
  };

  return (
    <Panel
      initiallyDeployed
      title={
        <>
          <BiMove className="text-muted" /> Node spacing
        </>
      }
    >
      <p className="text-small text-muted">Scale the exported layout outward or inward without regenerating the data.</p>
      <label htmlFor="spacing-scale" className="text-small">
        Spacing multiplier: <strong>{value.toFixed(1)}×</strong>
      </label>
      <input
        id="spacing-scale"
        type="range"
        min={MIN_SCALE}
        max={MAX_SCALE}
        step={STEP}
        value={value}
        onChange={handleChange}
      />
    </Panel>
  );
};

export default SpacingControl;
