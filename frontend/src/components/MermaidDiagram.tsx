import React, { useEffect, useRef, useState } from 'react';
import mermaid from 'mermaid';

mermaid.initialize({ 
    startOnLoad: false, 
    theme: 'dark', 
    securityLevel: 'loose',
    fontFamily: 'Inter, sans-serif'
});

interface MermaidDiagramProps {
  chart: string;
}

export const MermaidDiagram: React.FC<MermaidDiagramProps> = ({ chart }) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [svgStr, setSvgStr] = useState('');

  useEffect(() => {
    let isMounted = true;
    const renderChart = async () => {
      try {
        const id = `mermaid-${Math.random().toString(36).substring(7)}`;
        const { svg } = await mermaid.render(id, chart);
        if (isMounted) setSvgStr(svg);
      } catch (e) {
        console.error("Mermaid rendering error", e);
      }
    };
    renderChart();
    
    return () => { isMounted = false; };
  }, [chart]);

  return (
      <div 
        ref={containerRef} 
        dangerouslySetInnerHTML={{ __html: svgStr }} 
        style={{ 
            display: 'flex', 
            justifyContent: 'center', 
            width: '100%', 
            height: '100%',
            overflow: 'auto',
            padding: '1rem'
        }} 
      />
  );
};
