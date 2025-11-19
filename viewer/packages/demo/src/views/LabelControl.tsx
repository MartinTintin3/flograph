import { ChangeEvent, FC } from "react";
import { BiMove } from "react-icons/bi";

import Panel from "./Panel";
const STEP = 0.1;

interface LabelControlProps {
  value: number;
  onChange: (value: number) => void;
  min: number;
  max: number;
}

const LabelControl: FC<LabelControlProps> = ({ value, onChange, min, max }) => {
  const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
    onChange(Number(event.target.value));
  };

  return (
    <Panel
      initiallyDeployed
      title={
        <>
          <BiMove className="text-muted" /> Node Label
        </>
      }
    >
      <p className="text-small text-muted">Scale the exported layout outward or inward without regenerating the data.</p>
      <label htmlFor="Label-scale" className="text-small">
        Label multiplier: <strong>{value.toFixed(1)}×</strong>
      </label>
      <input
        id="Label-scale"
        type="range"
        min={min}
        max={max}
        step={STEP}
        value={value}
        onChange={handleChange}
      />
    </Panel>
  );
};

export default LabelControl;
