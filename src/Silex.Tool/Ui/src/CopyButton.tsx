import { useEffect, useRef, useState } from "react";
import { Check, Copy } from "lucide-react";

export function CopyButton({ label, text, copy, disabled = false, className = "copy-button", size = 13 }: { label: string; text: string; copy: (text: string) => Promise<boolean>; disabled?: boolean; className?: string; size?: number }) {
  const [copied, setCopied] = useState(false);
  const [pending, setPending] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout>>(undefined);
  const generation = useRef(0);
  useEffect(() => {
    setCopied(false); setPending(false);
    return () => { generation.current++; clearTimeout(timer.current); };
  }, [text]);
  async function copyValue() {
    const current = generation.current;
    setPending(true);
    const success = await copy(text);
    if (current !== generation.current) return;
    setPending(false);
    if (!success) return;
    clearTimeout(timer.current);
    setCopied(true);
    timer.current = setTimeout(() => setCopied(false), 2000);
  }
  return <button className={className} type="button" title={copied ? "Copied" : "Copy value"} aria-label={label} disabled={disabled || pending} onClick={() => void copyValue()}>{copied ? <Check size={size}/> : <Copy size={size}/>}</button>;
}
