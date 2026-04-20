import React, { useState } from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar from '../components/Sidebar'; 
import { signOut } from '../shared/authService';
import '../styles/Dashboard.css';

const QCLayout = ({ user, toggleTheme, isDarkMode }) => {
  const [isExpanded, setIsExpanded] = useState(true);

  const handleLogout = async () => {
    try {
      await signOut();
    } catch (err) {
      console.error('Logout failed:', err);
    }
  };

  return (
    <div style={{ height: '100vh', display: 'flex', flexDirection: 'column', backgroundColor: '#0B1220' }}>
       <div style={{ flexGrow: 1, overflow: 'hidden' }}>
          <Outlet context={{ handleLogout, user, toggleTheme, isDarkMode }} />
       </div>
    </div>
  );
};

export default QCLayout;
