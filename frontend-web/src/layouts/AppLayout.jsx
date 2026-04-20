import React, { useState } from 'react';
import { Outlet } from 'react-router-dom';
import Sidebar from '../components/Sidebar';
import Settings from '../components/pages/Settings';
import { signOut } from '../shared/authService';
import LedgerBuddy from '../components/chatbot/LedgerBuddy';
import '../styles/Dashboard.css';

const AppLayout = ({ user, toggleTheme, isDarkMode }) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);  // ← moved from Dashboard
  // ↑ activePage / setActivePage state REMOVED — React Router owns this now

  const handleLogout = async () => {
    try {
      const { error } = await signOut();
      if (error) console.error('Error signing out:', error.message);
    } catch (err) {
      console.error('Logout failed:', err);
    }
  };

  return (
    <div className="dashboard-shell">
      <Sidebar
        // activePage and onPageChange REMOVED
        isExpanded={isExpanded}
        onToggleExpand={() => setIsExpanded(!isExpanded)}
        user={user}
        toggleTheme={toggleTheme}
        isDarkMode={isDarkMode}
        onLogout={handleLogout}
        onOpenSettings={() => setIsSettingsOpen(true)}   // ← fixes Issue #4
      />
      <div className="dashboard-main">
        <div className="page-content">
          <Outlet />
          {/* context prop removed — pages use useNavigate/useLocation directly */}
        </div>
      </div>

      {isSettingsOpen && (
        <Settings onClose={() => setIsSettingsOpen(false)} />   // ← now reachable
      )}

      {/* LedgerBuddy AI Assistant */}
      {/* <LedgerBuddy user={user} isDarkMode={isDarkMode} /> */}
    </div>
  );
};

export default AppLayout;