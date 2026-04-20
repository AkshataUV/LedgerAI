import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import { supabase } from '../shared/supabase';
import { 
  Plus, Search, Tag, X, Loader2, 
  Trash2, Edit2, AlertCircle, CheckCircle, Upload, ChevronLeft, ChevronRight, FileText
} from 'lucide-react';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3000';
const PAGE_SIZE = 50;

/**
 * KeywordRulesTab Component
 * Supports manual entry and CSV bulk upload.
 * Implements 50-row pagination with grey/white curved styling.
 */
const KeywordRulesTab = () => {
    const [data, setData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [search, setSearch] = useState('');
    const [currentPage, setCurrentPage] = useState(1);
    const [showModal, setShowModal] = useState(false);
    const [error, setError] = useState(null);

    const fetchData = async () => {
        setLoading(true);
        try {
            const { data: { session } } = await supabase.auth.getSession();
            const res = await axios.get(`${API_BASE_URL}/api/qc/keyword-rules`, {
                headers: { Authorization: `Bearer ${session?.access_token}` }
            });
            setData(res.data || []);
            setError(null);
        } catch (e) {
            console.error('Failed to load rules:', e);
            setError('Failed to fetch keyword rules. Please check backend connection.');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => { fetchData(); }, []);

    const handleDelete = async (id) => {
        if (!window.confirm('Are you sure you want to delete this rule?')) return;
        try {
            const { data: { session } } = await supabase.auth.getSession();
            await axios.delete(`${API_BASE_URL}/api/qc/keyword-rules/${id}`, {
                headers: { Authorization: `Bearer ${session?.access_token}` }
            });
            fetchData();
        } catch (e) {
            alert('Failed to delete rule: ' + (e.response?.data?.error || e.message));
        }
    };

    const filtered = data.filter(row => row.keyword.toUpperCase().includes(search.toUpperCase()));
    const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
    const pageData = filtered.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

    // Pagination helper
    const renderPagination = () => {
        const pages = [];
        let start = Math.max(1, currentPage - 2);
        let end = Math.min(totalPages, start + 4);
        if (end - start < 4) start = Math.max(1, end - 4);

        for (let i = start; i <= end; i++) {
            pages.push(
                <button 
                  key={i} 
                  onClick={() => setCurrentPage(i)}
                  style={i === currentPage ? styles.pageBtnActive : styles.pageBtn}
                >
                    {i}
                </button>
            );
        }
        return (
            <div style={styles.pagination}>
                <button disabled={currentPage === 1} onClick={() => setCurrentPage(p => p - 1)} style={styles.navBtn}><ChevronLeft size={16}/></button>
                {pages}
                <button disabled={currentPage === totalPages} onClick={() => setCurrentPage(p => p + 1)} style={styles.navBtn}><ChevronRight size={16}/></button>
            </div>
        );
    };

    return (
        <div style={styles.container}>
            {showModal && <AddRuleModal onClose={() => setShowModal(false)} onSuccess={fetchData} />}
            
            <header style={styles.header}>
                <div style={styles.headerTitleGroup}>
                   <div style={styles.iconBox}><Tag size={18} color="#fff"/></div>
                   <div>
                      <h2 style={styles.title}>Global Keyword Rules</h2>
                      <p style={styles.subtitle}>Deterministic text-matching categorization logic</p>
                   </div>
                </div>
                <div style={styles.headerActions}>
                   <div style={styles.searchBox}>
                      <Search size={14} style={styles.searchIcon}/>
                      <input 
                        type="text" 
                        placeholder="Search rules..." 
                        style={styles.searchInput} 
                        value={search} 
                        onChange={e => { setSearch(e.target.value); setCurrentPage(1); }} 
                      />
                   </div>
                   <button style={styles.btnPrimary} onClick={() => setShowModal(true)}>
                      <Plus size={16}/> Add Rule / CSV
                   </button>
                </div>
            </header>

            <div style={styles.tableCard}>
                <div style={styles.tableHeader}>
                    <div style={{ flex: 2 }}>KEYWORD</div>
                    <div style={{ flex: 1 }}>TEMPLATE</div>
                    <div style={{ flex: 1 }}>MATCH</div>
                    <div style={{ flex: 0.8 }}>PRIORITY</div>
                    <div style={{ flex: 0.8 }}>HITS</div>
                    <div style={{ flex: 1 }}>STATUS</div>
                    <div style={{ width: '60px', textAlign: 'center' }}>ACTIONS</div>
                </div>
                <div style={styles.tableBody}>
                    {loading ? (
                        <div style={styles.loaderArea}>
                            <Loader2 size={32} style={{ animation: 'spin 2s linear infinite', color: '#6366f1' }}/>
                            <p>Loading rules...</p>
                        </div>
                    ) : error ? (
                        <div style={styles.errorArea}><AlertCircle size={24}/> {error}</div>
                    ) : pageData.length === 0 ? (
                        <div style={styles.emptyArea}>No matching rules found.</div>
                    ) : (
                        pageData.map(row => (
                            <div key={row.keyword_id} style={styles.tableRow}>
                                <div style={{ flex: 2, fontWeight: 700, color: '#fff' }}>{row.keyword}</div>
                                <div style={{ flex: 1 }}><span style={styles.idBadge}>ID: {row.target_template_id}</span></div>
                                <div style={{ flex: 1 }}><span style={styles.typeBadge}>{row.match_type}</span></div>
                                <div style={{ flex: 0.8 }}>{row.priority}</div>
                                <div style={{ flex: 0.8 }}>{row.hit_count}</div>
                                <div style={{ flex: 1 }}>
                                    <span style={row.is_active ? styles.statusActive : styles.statusInactive}>
                                        {row.is_active ? 'ACTIVE' : 'DISABLED'}
                                    </span>
                                </div>
                                <div style={{ width: '60px', display: 'flex', justifyContent: 'center' }}>
                                    <button style={styles.iconBtn} onClick={() => handleDelete(row.keyword_id)}><Trash2 size={14}/></button>
                                </div>
                            </div>
                        ))
                    )}
                </div>
                {totalPages > 1 && renderPagination()}
            </div>
        </div>
    );
};

const AddRuleModal = ({ onClose, onSuccess }) => {
    const [mode, setMode] = useState('manual');
    const [formData, setFormData] = useState({
        keyword: '',
        target_template_id: '',
        match_type: 'CONTAINS',
        priority: 90,
        is_active: true
    });
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const fileInputRef = useRef(null);

    const handleSubmit = async (e) => {
        if (e) e.preventDefault();
        if (mode === 'manual' && !formData.keyword) return setError('Keyword is required');
        
        setLoading(true);
        try {
            const { data: { session } } = await supabase.auth.getSession();
            await axios.post(`${API_BASE_URL}/api/qc/keyword-rules`, {
                ...formData,
                target_template_id: formData.target_template_id ? Number(formData.target_template_id) : null,
                priority: Number(formData.priority)
            }, {
                headers: { Authorization: `Bearer ${session?.access_token}` }
            });
            onSuccess();
            onClose();
        } catch (err) {
            setError(err.response?.data?.error || 'Failed to create rule');
        } finally {
            setLoading(false);
        }
    };

    const handleCsvUpload = async (e) => {
        const file = e.target.files[0];
        if (!file) return;

        const reader = new FileReader();
        reader.onload = async (event) => {
            const text = event.target.result;
            const lines = text.split('\n').filter(l => l.trim());
            const rules = [];
            
            // Expected CSV: keyword, template_id
            lines.forEach((line, index) => {
                const [keyword, template_id] = line.split(',').map(s => s.trim());
                if (keyword && keyword !== 'keyword') {
                    rules.push({ keyword, target_template_id: Number(template_id) || null });
                }
            });

            if (rules.length === 0) return setError('No valid rules found in CSV');

            setLoading(true);
            try {
                const { data: { session } } = await supabase.auth.getSession();
                await axios.post(`${API_BASE_URL}/api/qc/keyword-rules/bulk`, { rules }, {
                    headers: { Authorization: `Bearer ${session?.access_token}` }
                });
                onSuccess();
                onClose();
            } catch (err) {
                setError('CSV Upload failed: ' + (err.response?.data?.error || err.message));
            } finally {
                setLoading(false);
            }
        };
        reader.readAsText(file);
    };

    return (
        <div style={styles.modalOverlay}>
            <div style={styles.modalContent}>
                <header style={styles.modalHeader}>
                    <div style={{ display: 'flex', gap: '1rem' }}>
                        <button 
                            onClick={() => setMode('manual')} 
                            style={mode === 'manual' ? styles.tabBtnActive : styles.tabBtn}
                        >Manual</button>
                        <button 
                            onClick={() => setMode('csv')} 
                            style={mode === 'csv' ? styles.tabBtnActive : styles.tabBtn}
                        >CSV Upload</button>
                    </div>
                    <button onClick={onClose} style={styles.closeBtn}><X size={18}/></button>
                </header>

                <div style={styles.modalBody}>
                    {mode === 'manual' ? (
                        <form onSubmit={handleSubmit}>
                            <div style={styles.formGroup}>
                                <label>Keyword / Text</label>
                                <input 
                                  type="text" 
                                  placeholder="e.g. STARBUCKS"
                                  value={formData.keyword}
                                  onChange={e => setFormData({...formData, keyword: e.target.value})}
                                  style={styles.formInput}
                                />
                            </div>
                            <div style={styles.formRow}>
                                <div style={styles.formGroup}>
                                    <label>Template ID</label>
                                    <input 
                                      type="number" 
                                      value={formData.target_template_id}
                                      onChange={e => setFormData({...formData, target_template_id: e.target.value})}
                                      style={styles.formInput}
                                    />
                                </div>
                                <div style={styles.formGroup}>
                                    <label>Priority</label>
                                    <input 
                                      type="number" 
                                      value={formData.priority}
                                      onChange={e => setFormData({...formData, priority: e.target.value})}
                                      style={styles.formInput}
                                    />
                                </div>
                            </div>
                            {error && <div style={styles.errorMsg}><AlertCircle size={14}/> {error}</div>}
                            <button type="submit" disabled={loading} style={styles.submitBtn}>
                                {loading ? <Loader2 size={16} className="spin"/> : 'Add Rule'}
                            </button>
                        </form>
                    ) : (
                        <div style={styles.csvArea}>
                            <div style={styles.csvIcon}><FileText size={48} opacity={0.2}/></div>
                            <p style={{ fontSize: '13px', opacity: 0.7, textAlign: 'center' }}>
                                Upload a CSV file with columns: <br/>
                                <code style={{ color: '#8b5cf6' }}>keyword, template_id</code>
                            </p>
                            <input 
                                type="file" 
                                accept=".csv" 
                                ref={fileInputRef} 
                                style={{ display: 'none' }} 
                                onChange={handleCsvUpload}
                            />
                            <button 
                                style={styles.secondaryBtn} 
                                onClick={() => fileInputRef.current.click()}
                                disabled={loading}
                            >
                                {loading ? <Loader2 size={16} className="spin"/> : <Upload size={16}/>}
                                Choose CSV File
                            </button>
                            {error && <div style={{ ...styles.errorMsg, marginTop: '1rem' }}><AlertCircle size={14}/> {error}</div>}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

const styles = {
    container: { display: 'flex', flexDirection: 'column', height: '100%', minWidth: 0 },
    header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.25rem' },
    headerTitleGroup: { display: 'flex', alignItems: 'center', gap: '1rem' },
    iconBox: { width: '40px', height: '40px', background: 'linear-gradient(135deg, #10b981, #059669)', borderRadius: '12px', display: 'flex', alignItems: 'center', justifyContent: 'center' },
    title: { fontSize: '1.1rem', fontWeight: 800, color: '#fff', margin: 0 },
    subtitle: { fontSize: '11px', opacity: 0.5, margin: 0 },
    headerActions: { display: 'flex', gap: '0.75rem', alignItems: 'center' },
    
    searchBox: { display: 'flex', alignItems: 'center', gap: '8px', background: 'rgba(0,0,0,0.2)', border: '1px solid rgba(255,255,255,0.05)', borderRadius: '10px', padding: '0.4rem 0.75rem', width: '220px' },
    searchIcon: { opacity: 0.3 },
    searchInput: { background: 'transparent', border: 'none', color: '#fff', fontSize: '12px', outline: 'none', width: '100%' },
    btnPrimary: { display: 'flex', alignItems: 'center', gap: '8px', background: 'linear-gradient(135deg, #6366f1, #4f46e5)', border: 'none', color: '#fff', padding: '0.5rem 1rem', borderRadius: '10px', fontSize: '12px', fontWeight: 700, cursor: 'pointer' },
    
    tableCard: { background: 'rgba(15, 23, 42, 0.4)', borderRadius: '16px', display: 'flex', flexDirection: 'column', overflow: 'hidden', flexGrow: 1, border: '1px solid rgba(255,255,255,0.03)' },
    tableHeader: { display: 'flex', padding: '1rem 1.5rem', background: 'rgba(0,0,0,0.3)', color: '#64748b', fontSize: '10px', fontWeight: 900, textTransform: 'uppercase' },
    tableBody: { overflowY: 'auto', flexGrow: 1 },
    tableRow: { display: 'flex', padding: '0.85rem 1.5rem', borderBottom: '1px solid rgba(255,255,255,0.02)', fontSize: '13px', color: '#94a3b8', alignItems: 'center' },
    
    idBadge: { background: 'rgba(99, 102, 241, 0.1)', color: '#818cf8', padding: '2px 8px', borderRadius: '4px', fontWeight: 700, fontSize: '10px' },
    typeBadge: { background: 'rgba(255,255,255,0.05)', padding: '2px 8px', borderRadius: '4px', fontSize: '10px' },
    statusActive: { color: '#10b981', fontWeight: 800, fontSize: '10px' },
    statusInactive: { color: '#ef4444', fontWeight: 800, fontSize: '10px' },
    iconBtn: { background: 'transparent', border: 'none', color: '#475569', cursor: 'pointer' },
    
    pagination: { padding: '0.85rem', display: 'flex', justifyContent: 'center', alignItems: 'center', gap: '0.4rem', background: 'rgba(0,0,0,0.2)' },
    pageBtn: { minWidth: '32px', height: '32px', background: '#334155', border: 'none', color: '#94a3b8', borderRadius: '8px', cursor: 'pointer', fontSize: '12px', fontWeight: 600 },
    pageBtnActive: { minWidth: '32px', height: '32px', background: '#fff', border: 'none', color: '#0f172a', borderRadius: '8px', cursor: 'pointer', fontSize: '12px', fontWeight: 800 },
    navBtn: { width: '32px', height: '32px', background: 'transparent', border: 'none', color: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center', cursor: 'pointer', opacity: 0.5 },
    
    loaderArea: { height: '300px', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: '1rem', color: '#64748b' },
    errorArea: { height: '300px', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#ef4444' },
    emptyArea: { padding: '4rem', textAlign: 'center', color: '#475569' },

    modalOverlay: { position: 'fixed', inset: 0, background: 'rgba(2, 6, 23, 0.85)', backdropFilter: 'blur(10px)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 },
    modalContent: { background: '#1e293b', borderRadius: '24px', width: '420px', overflow: 'hidden' },
    modalHeader: { padding: '1.25rem', borderBottom: '1px solid rgba(255,255,255,0.05)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' },
    tabBtn: { background: 'transparent', border: 'none', color: '#64748b', padding: '0.5rem 1rem', fontSize: '13px', fontWeight: 600, cursor: 'pointer' },
    tabBtnActive: { background: 'rgba(99, 102, 241, 0.1)', border: 'none', color: '#fff', padding: '0.5rem 1rem', fontSize: '13px', borderRadius: '8px', fontWeight: 700, cursor: 'pointer' },
    closeBtn: { background: 'transparent', border: 'none', color: '#64748b', cursor: 'pointer' },
    modalBody: { padding: '1.5rem' },
    formGroup: { marginBottom: '1rem', display: 'flex', flexDirection: 'column', gap: '6px' },
    formRow: { display: 'flex', gap: '1rem' },
    formInput: { background: 'rgba(0,0,0,0.3)', border: '1px solid rgba(255,255,255,0.05)', borderRadius: '10px', padding: '0.75rem', color: '#fff', fontSize: '14px', outline: 'none' },
    submitBtn: { width: '100%', background: 'linear-gradient(135deg, #6366f1, #4f46e5)', border: 'none', color: '#fff', padding: '0.85rem', borderRadius: '12px', fontWeight: 700, cursor: 'pointer', marginTop: '1rem' },
    secondaryBtn: { width: '100%', background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.1)', color: '#fff', padding: '0.85rem', borderRadius: '12px', fontWeight: 600, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '8px' },
    errorMsg: { color: '#ef4444', fontSize: '12px', marginBottom: '1rem' },
    csvArea: { display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '1rem', padding: '1rem 0' },
    csvIcon: { width: '80px', height: '80px', background: 'rgba(139, 92, 246, 0.1)', borderRadius: '20px', display: 'flex', alignItems: 'center', justifyContent: 'center' }
};

export default KeywordRulesTab;
