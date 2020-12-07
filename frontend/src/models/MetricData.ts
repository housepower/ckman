export interface MetricData {
  metric: {
    gc?: string;
    instance?: string;
    job?: string;
    __name__?: string;
    device?: string;
    task?: string;
  };
  values: [number, string];
}