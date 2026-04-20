import React from 'react';
import { Navigate, Outlet } from 'react-router-dom';
import { useAuth } from '../shared/hooks/useAuth';
import { useRole } from '../context/RoleContext';

const ProtectedRoute = ({ allowedRoles, children }) => {
  const { user, loading: authLoading } = useAuth();
  const { role, roleLoading } = useRole();

  if (authLoading || roleLoading) {
    return <div style={{ height: '100vh', backgroundColor: '#0B1220' }} />;
  }

  if (!user) {
    return <Navigate to="/auth" replace />;
  }

  if (allowedRoles && !allowedRoles.includes(role)) {
    if (role === 'QC') return <Navigate to="/qc" replace />;
    return <div style={{ minHeight: '100vh', display: 'grid', placeItems: 'center', backgroundColor: '#0f172a', color: '#e2e8f0', fontFamily: 'Inter, sans-serif' }}>
      <div style={{ textAlign: 'center' }}>
        <h2 style={{ marginBottom: '8px' }}>Access Restricted</h2>
        <p style={{ color: '#94a3b8' }}>Your role ({role}) does not have permission to view this page.</p>
        <button onClick={() => window.location.href = '/'} style={{ marginTop: '16px', padding: '8px 16px', borderRadius: '6px', background: '#483EA8', color: 'white', border: 'none', cursor: 'pointer' }}>Go Home</button>
      </div>
    </div>;
  }

  return children ? children : <Outlet />;
};

export default ProtectedRoute;
