import React, { useState, useCallback } from 'react';

const useToast = () => {
  const [toasts, setToasts] = useState([]);
  const toastIdRef = React.useRef(0);

  const showToast = useCallback((message, type = 'success') => {
    const id = ++toastIdRef.current;
    const newToast = { id, message, type };

    setToasts((prev) => [...prev, newToast]);

    // Auto-dismiss after 3 seconds
    const timeoutId = setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, 3000);

    return {
      dismiss: () => {
        clearTimeout(timeoutId);
        setToasts((prev) => prev.filter((t) => t.id !== id));
      }
    };
  }, []);

  const dismissToast = useCallback((id) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  return { toasts, showToast, dismissToast };
};

const Toast = ({ toasts, onDismiss }) => {
  const handleDismiss = (id) => {
    if (onDismiss) {
      onDismiss(id);
    }
  };

  return (
    <div
      style={{
        position: 'fixed',
        bottom: '20px',
        right: '20px',
        zIndex: 9999,
        display: 'flex',
        flexDirection: 'column',
        gap: '12px',
        pointerEvents: 'none'
      }}
    >
      {toasts.map((toast) => (
        <div
          key={toast.id}
          style={{
            backgroundColor: 'var(--bg-secondary)',
            border: '1px solid var(--glass-border)',
            borderLeft: toast.type === 'success' ? '4px solid #10B981' : '4px solid #F87171',
            borderRadius: '12px',
            padding: '16px',
            minWidth: '320px',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            color: toast.type === 'success' ? '#10B981' : '#F87171',
            fontSize: '14px',
            fontWeight: '500',
            boxShadow: '0 8px 24px rgba(0, 0, 0, 0.3)',
            pointerEvents: 'auto',
            animation: 'slideIn 0.3s cubic-bezier(0.16, 1, 0.3, 1) forwards',
          }}
        >
          <span>{toast.message}</span>
          <button
            onClick={() => handleDismiss(toast.id)}
            style={{
              background: 'none',
              border: 'none',
              color: 'inherit',
              cursor: 'pointer',
              fontSize: '18px',
              padding: '0 0 0 12px',
              display: 'flex',
              alignItems: 'center',
              transition: 'opacity 0.2s'
            }}
            onMouseEnter={(e) => (e.target.style.opacity = '0.7')}
            onMouseLeave={(e) => (e.target.style.opacity = '1')}
          >
            ✕
          </button>
        </div>
      ))}

      <style>{`
        @keyframes slideIn {
          from {
            opacity: 0;
            transform: translateX(400px);
          }
          to {
            opacity: 1;
            transform: translateX(0);
          }
        }
      `}</style>
    </div>
  );
};

export { Toast, useToast };
export default Toast;
