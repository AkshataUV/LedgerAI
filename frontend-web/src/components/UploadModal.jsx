import React, { useState } from 'react';
import { supabase } from '../shared/supabase';
import '../styles/UploadModal.css';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3000';

const UploadModal = ({ onClose, onUploadSuccess }) => {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState(false);
  const [isDragging, setIsDragging] = useState(false);

  const handleDragOver = (e) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    setIsDragging(false);
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    const uploadedFile = e.dataTransfer.files[0];
    if (uploadedFile && uploadedFile.type === 'application/json') {
      setFile(uploadedFile);
      setError('');
    } else {
      setError('Please upload a valid JSON file.');
    }
  };

  const handleFileChange = (e) => {
    const uploadedFile = e.target.files[0];
    if (uploadedFile) {
      setFile(uploadedFile);
      setError('');
    }
  };

  const handleUpload = async () => {
    console.log('📡 API_BASE_URL:', API_BASE_URL);

    if (!file) {
      setError('Please select a file first.');
      return;
    }

    setLoading(true);
    setError('');

    try {
      if (!supabase) {
        setError('Supabase is not configured. Check VITE_SUPABASE_URL and VITE_SUPABASE_ANON_KEY.');
        setLoading(false);
        return;
      }

      const { data: { session } } = await supabase.auth.getSession();
      if (!session?.access_token) {
        throw new Error('Authentication required. Please sign in again.');
      }

      const fileContent = await file.text();
      const jsonData = JSON.parse(fileContent);

      if (!jsonData.file_name || !jsonData.transactions) {
        throw new Error("Invalid JSON format. Must contain file_name and transactions list.");
      }

      if (!Array.isArray(jsonData.transactions) || jsonData.transactions.length === 0) {
        throw new Error("Transactions must be a non-empty array.");
      }

      // Call backend endpoint for atomic bulk upload
      const response = await fetch(`${API_BASE_URL}/api/transactions/upload-bulk`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${session.access_token}`
        },
        body: JSON.stringify({
          file_name: jsonData.file_name,
          transactions: jsonData.transactions,
          identifiers: jsonData.identifiers
        })
      });

      console.log('📬 Upload response status:', response.status, response.statusText);

      if (!response.ok) {
        const errorPayload = await response.json().catch(() => ({}));
        throw new Error(errorPayload.error || 'Failed to upload statement.');
      }

      setSuccess(true);
      if (onUploadSuccess) onUploadSuccess();
      setTimeout(onClose, 1500); // Close after showing success
    } catch (err) {
      console.error('Upload failed:', err);
      setError(err.message || 'Failed to parse or upload JSON file.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="upload-modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h2>Upload Statement</h2>
          <button className="close-modal-btn" onClick={onClose}>✕</button>
        </div>

        <div className="modal-body">
          {error && <div className="error-banner">{error}</div>}
          {success && <div className="success-banner">✓ Uploaded Successfully!</div>}

          <div 
            className={`drop-zone ${isDragging ? 'dragging' : ''} ${file ? 'has-file' : ''}`}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onDrop={handleDrop}
          >
            <div className="drop-content">
              <span className="drop-icon">{file ? '📄' : '📤'}</span>
              {file ? (
                <div className="file-info">
                  <p className="file-name">{file.name}</p>
                  <p className="file-size">{(file.size / 1024).toFixed(2)} KB</p>
                </div>
              ) : (
                <>
                  <p className="primary-text">Drag and drop file here</p>
                  <span className="or-text">or</span>
                </>
              )}
              <label className="browse-btn">
                {file ? 'Change File' : 'Browse Files'}
                <input type="file" accept=".json" onChange={handleFileChange} hidden />
              </label>
              <p className="support-text">Only supports .json for now</p>
            </div>
          </div>
        </div>

        <div className="modal-footer">
          <button className="cancel-btn" onClick={onClose} disabled={loading}>Cancel</button>
          <button 
            className="submit-btn" 
            onClick={handleUpload} 
            disabled={!file || loading || success}
          >
            {loading ? <span className="spinner"></span> : success ? 'Uploaded' : 'Upload'}
          </button>
        </div>
      </div>
    </div>
  );
};

export default UploadModal;
