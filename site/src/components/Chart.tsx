import { useEffect, useRef } from "react";
import * as echarts from "echarts";

type ChartProps = {
  option: Record<string, unknown>;
  className?: string;
};

export function Chart({ option, className }: ChartProps) {
  const ref = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!ref.current) {
      return;
    }

    const instance = echarts.init(ref.current, undefined, { renderer: "canvas" });
    instance.setOption(option as echarts.EChartsOption);

    const observer = new ResizeObserver(() => instance.resize());
    observer.observe(ref.current);

    return () => {
      observer.disconnect();
      instance.dispose();
    };
  }, [option]);

  return <div ref={ref} className={className ?? "h-80 w-full"} />;
}
