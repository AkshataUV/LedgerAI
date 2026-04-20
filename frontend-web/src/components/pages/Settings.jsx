import React from 'react';

const Settings = ({ onClose }) => {
  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="settings-modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>Settings</h2>
          <button className="close-modal-btn" onClick={onClose}>✕</button>
        </div>
        <div className="modal-body">
          <p style={{ color: 'var(--text-secondary)' }}>Manage your profile options, connected accounts, and preferences here.</p>
        </div>
      </div>
    </div>
  );
};

export default Settings;
