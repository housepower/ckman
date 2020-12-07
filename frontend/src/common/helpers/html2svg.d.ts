export function html2svg(
  element: HTMLElement,
  styles?: Record<string, any>,
  size?: {
    width: string | number;
    height: string | number;
  },
): Promise<SVGSVGElement>;
