import React from 'react';
import { Outlet } from 'react-router-dom';

const AuthLayout = () => {
  return (
    <div style={{ height: '100vh', backgroundColor: '#0B1220' }}>
      <Outlet />
    </div>
  );
};

export default AuthLayout;
