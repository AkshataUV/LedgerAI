import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { supabase } from '../shared/supabase';
import { 
  FileCheck, Loader2, FileText, CheckCircle2, 
  AlertTriangle, Search, ChevronRight, Activity
} from 'lucide-react';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3000';

const ReviewDocumentTab = () => {
    const [documents, setDocuments] = useState([]);
    const [loading, setLoading] = useState(true);
    const [selectedDoc, setSelectedDoc] = useState(null);
    const [error, setError] = useState(null);

    useEffect(() => {
        const fetchDocs = async () => {
            try {
                const { data: { session } } = await supabase.auth.getSession();
                const res = await axios.get(`${API_BASE_URL}/api/qc/review-documents`, {
                    headers: { Authorization: `Bearer ${session?.access_token}` }
                });
                setDocuments(res.data || []);
            } catch (err) {
                setError('Failed to load review documents');
            } finally {
                setLoading(false);
            }
        };
        fetchDocs();
    }, []);

    if (loading) return (
        <div style={styles.center}><Loader2 className="spin" size={32} color="#6366f1"/></div>
    );

    return (
        <div style={styles.container}>
            <header style={styles.header}>
                <div style={styles.headerLeft}>
                    <div style={styles.iconBox}><FileCheck size={18} color="#fff"/></div>
                    <div>
                        <h2 style={styles.title}>Review Documents</h2>
                        <p style={styles.subtitle}>Formats with <span style={{ color: '#f59e0b' }}>EXPERIMENTAL</span> status requiring verification</p>
                    </div>
                </div>
            </header>

            <div style={styles.content}>
                <div style={styles.docList}>
                    {documents.length === 0 ? (
                        <div style={styles.empty}>No documents awaiting review.</div>
                    ) : (
                        documents.map(doc => (
                            <div 
                                key={doc.document_id} 
                                style={selectedDoc?.document_id === doc.document_id ? styles.docItemActive : styles.docItem}
                                onClick={() => setSelectedDoc(doc)}
                            >
                                <div style={styles.docInfo}>
                                    <FileText size={16} opacity={0.5}/>
                                    <div style={{ flex: 1 }}>
                                        <div style={styles.docName}>{doc.file_name}</div>
                                        <div style={styles.docMeta}>{doc.institution_name} • {doc.statement_type}</div>
                                    </div>
                                    <div style={styles.statusBadge}>{doc.format_status}</div>
                                    <ChevronRight size={14} opacity={0.3}/>
                                </div>
                            </div>
                        ))
                    )}
                </div>

                <div style={styles.docDetails}>
                    {selectedDoc ? (
                        <div style={styles.detailsView}>
                            <div style={styles.detailsHeader}>
                                <h3>{selectedDoc.file_name}</h3>
                                <div style={styles.detailsActions}>
                                    <button style={styles.btnAction}><CheckCircle2 size={14}/> Approve Format</button>
                                </div>
                            </div>
                            <div style={styles.detailsGrid}>
                                <div style={styles.logicCard}>
                                    <h4>Extraction Logic</h4>
                                    <pre style={styles.codeBlock}>
                                        {/* Simplified logic view */}
                                        {"# Python regex based extraction logic\nimport re\n...\n"}
                                    </pre>
                                </div>
                                <div style={styles.statsCard}>
                                    <div style={styles.statNode}>
                                        <span style={styles.statLabel}>Total Transactions</span>
                                        <span style={styles.statValue}>142</span>
                                    </div>
                                    <div style={styles.statNode}>
                                        <span style={styles.statLabel}>Match Accuracy</span>
                                        <span style={{ ...styles.statValue, color: '#10b981' }}>98.5%</span>
                                    </div>
                                </div>
                            </div>
                            <div style={styles.comparisonArea}>
                                <div style={styles.tableHeader}>
                                    <div style={{ flex: 1 }}>Date</div>
                                    <div style={{ flex: 3 }}>Description</div>
                                    <div style={{ flex: 1 }}>Amount</div>
                                    <div style={{ flex: 1 }}>Status</div>
                                </div>
                                {/* Placeholder for transaction rows */}
                                <div style={styles.tableRow}>
                                    <div style={{ flex: 1 }}>2024-03-01</div>
                                    <div style={{ flex: 3 }}>AMAZON RETAIL SEATTLE</div>
                                    <div style={{ flex: 1, color: '#ef4444' }}>-45.00</div>
                                    <div style={{ flex: 1, color: '#10b981' }}>✓ Matched</div>
                                </div>
                            </div>
                        </div>
                    ) : (
                        <div style={styles.detailsEmpty}>
                            <Activity size={48} opacity={0.1}/>
                            <p>Select a document from the list to review extraction logic</p>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

const styles = {
    container: { height: '100%', display: 'flex', flexDirection: 'column' },
    header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' },
    headerLeft: { display: 'flex', alignItems: 'center', gap: '1rem' },
    iconBox: { width: '40px', height: '40px', background: 'linear-gradient(135deg, #f59e0b, #d97706)', borderRadius: '12px', display: 'flex', alignItems: 'center', justifyContent: 'center' },
    title: { fontSize: '1.1rem', fontWeight: 800, color: '#fff', margin: 0 },
    subtitle: { fontSize: '11px', opacity: 0.5, margin: 0 },
    
    content: { flexGrow: 1, display: 'flex', gap: '1.5rem', minHeight: 0 },
    docList: { width: '380px', flexShrink: 0, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: '0.75rem' },
    docItem: { background: 'rgba(15, 23, 42, 0.4)', padding: '1rem', borderRadius: '16px', cursor: 'pointer', border: '1px solid transparent', transition: 'all 0.2s' },
    docItemActive: { background: 'rgba(99, 102, 241, 0.1)', padding: '1rem', borderRadius: '16px', cursor: 'pointer', border: '1px solid rgba(99, 102, 241, 0.3)', boxShadow: '0 8px 24px rgba(0,0,0,0.2)' },
    docInfo: { display: 'flex', alignItems: 'center', gap: '1rem' },
    docName: { fontSize: '13px', fontWeight: 700, color: '#fff' },
    docMeta: { fontSize: '11px', color: '#64748b', marginTop: '2px' },
    statusBadge: { fontSize: '9px', fontWeight: 900, background: 'rgba(245, 158, 11, 0.1)', color: '#f59e0b', padding: '2px 6px', borderRadius: '4px' },
    
    docDetails: { flexGrow: 1, background: 'rgba(15, 23, 42, 0.2)', borderRadius: '24px', border: '1px solid rgba(255,255,255,0.03)', overflow: 'hidden', display: 'flex', flexDirection: 'column' },
    detailsView: { padding: '1.5rem', display: 'flex', flexDirection: 'column', height: '100%', gap: '1.5rem' },
    detailsHeader: { display: 'flex', justifyContent: 'space-between', alignItems: 'center' },
    detailsActions: { display: 'flex', gap: '0.75rem' },
    btnAction: { background: '#10b981', border: 'none', color: '#fff', padding: '0.5rem 1rem', borderRadius: '10px', fontSize: '12px', fontWeight: 700, cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px' },
    
    detailsGrid: { display: 'grid', gridTemplateColumns: '1fr 240px', gap: '1.5rem' },
    logicCard: { background: '#020617', padding: '1rem', borderRadius: '16px', border: '1px solid rgba(255,255,255,0.05)' },
    codeBlock: { margin: 0, fontSize: '12px', color: '#a5b4fc', fontFamily: "'Fira Code', monospace", opacity: 0.8 },
    statsCard: { display: 'flex', flexDirection: 'column', gap: '1rem' },
    statNode: { background: 'rgba(255,255,255,0.02)', padding: '1rem', borderRadius: '16px', display: 'flex', flexDirection: 'column', gap: '4px' },
    statLabel: { fontSize: '10px', color: '#64748b', textTransform: 'uppercase', fontWeight: 700 },
    statValue: { fontSize: '20px', fontWeight: 900, color: '#fff' },

    comparisonArea: { flexGrow: 1, overflowY: 'auto' },
    tableHeader: { display: 'flex', padding: '0.75rem 1rem', background: 'rgba(0,0,0,0.2)', borderRadius: '8px', marginBottom: '0.5rem', fontSize: '10px', fontWeight: 900, textTransform: 'uppercase', color: '#475569' },
    tableRow: { display: 'flex', padding: '0.75rem 1rem', borderBottom: '1px solid rgba(255,255,255,0.02)', fontSize: '13px', color: '#94a3b8' },

    detailsEmpty: { height: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: '1rem', opacity: 0.3, textAlign: 'center' },
    center: { height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' },
    empty: { padding: '2rem', textAlign: 'center', opacity: 0.3 }
};

export default ReviewDocumentTab;
