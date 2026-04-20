import React, { useState, useEffect } from 'react';
import { useOutletContext } from 'react-router-dom';
import { supabase } from '../shared/supabase';
import { 
  Package, Tag, Database, History, 
  LayoutDashboard, Loader2, LogOut, ShieldCheck,
  FileText, Zap, ShieldAlert
} from 'lucide-react';

// Subcomponents
import CoaLibraryTab from './CoaLibraryTab';
import KeywordRulesTab from './KeywordRulesTab';
import VectorCacheTab from './VectorCacheTab';
import ReviewDocumentTab from './ReviewDocumentTab';
import RandomQCTab from './RandomQCTab';
import FrequentlyChangedTab from './FrequentlyChangedTab';

const CAT_COLORS = {
  'MANUAL': '#3b82f6',
  'G_RULE': '#10b981',
  'FILTER': '#f59e0b',
  'P_EXACT': '#8b5cf6',
  'P_VEC': '#a855f7',
  'G_VEC': '#ec4899',
  'LLM': '#ef4444',
  'UNKNOWN': '#94a3b8'
};

const QCPanel = () => {
  const { handleLogout, user } = useOutletContext() || {};
  const [activeTab, setActiveTab] = useState('COA_LIBRARY');
  const [stats, setStats] = useState({});
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStats = async () => {
      try {
        const { data, error } = await supabase.from('transactions').select('categorised_by');
        if (error) throw error;
        const grouped = (data || []).reduce((acc, curr) => {
          const cat = curr.categorised_by || 'UNKNOWN';
          acc[cat] = (acc[cat] || 0) + 1;
          return acc;
        }, {});
        setStats(grouped);
        setTotal(Object.values(grouped).reduce((a, b) => a + b, 0));
      } catch (err) {
        console.error('Failed to fetch QC stats:', err);
      } finally {
        setLoading(false);
      }
    };
    fetchStats();
  }, []);

  return (
    <div style={styles.container}>
      {/* ── Modern Header ── */}
      <header style={styles.header}>
        <div style={styles.brand}>
          <div style={styles.brandIcon}><ShieldCheck size={20} color="#fff"/></div>
          <div>
            <div style={styles.brandName}>LEDGERAI / QC</div>
            <div style={styles.brandSub}>Quality Control & Management</div>
          </div>
        </div>

        {/* Global Stats Overview */}
        {!loading && (
          <div style={styles.statsOverview}>
             <div style={styles.statBar}>
                {Object.entries(stats).map(([cat, count]) => (
                  <div 
                    key={cat} 
                    style={{ 
                        width: total ? `${(count/total)*100}%` : '0%', 
                        backgroundColor: CAT_COLORS[cat] || CAT_COLORS.UNKNOWN, 
                        height: '100%',
                        transition: 'width 0.5s ease'
                    }} 
                    title={`${cat}: ${count} transactions`} 
                  />
                ))}
             </div>
             <div style={styles.statLegend}>
                {Object.entries(CAT_COLORS).map(([cat, color]) => (
                    <div key={cat} style={styles.legendNode}>
                        <div style={{ ...styles.dot, backgroundColor: color }} />
                        <span>{cat}: <b>{stats[cat] || 0}</b></span>
                    </div>
                ))}
             </div>
          </div>
        )}

        <div style={styles.userProfile}>
           <span style={styles.userEmail}>{user?.email?.split('@')[0]}</span>
           <button style={styles.logoutBtn} onClick={handleLogout} title="Logout">
              <LogOut size={16}/>
           </button>
        </div>
      </header>

      {/* ── Secondary Nav ── */}
      <nav style={styles.navBar}>
        {[
          { id: 'COA_LIBRARY', label: 'COA Library', icon: <Package size={14}/> },
          { id: 'REVIEW_DOCS', label: 'Review Documents', icon: <FileText size={14}/> },
          { id: 'RANDOM_QC', label: 'Random QC', icon: <ShieldCheck size={14}/> },
          { id: 'FREQ_CHANGED', label: 'Frequently Changed', icon: <Zap size={14}/> },
          { id: 'AUDIT_QUEUE', label: 'Audit Queue', icon: <History size={14}/> },
          { id: 'KEYWORD_RULES', label: 'Keyword Rules', icon: <Tag size={14}/> },
          { id: 'VECTOR_CACHE', label: 'Vector Cache', icon: <Database size={14}/> },
        ].map(tab => (
          <button 
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            style={activeTab === tab.id ? styles.navLinkActive : styles.navLink}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </nav>

      {/* ── Main Workspace ── */}
      <main style={styles.workspace}>
        {activeTab === 'COA_LIBRARY' && <CoaLibraryTab />}
        {activeTab === 'REVIEW_DOCS' && <ReviewDocumentTab />}
        {activeTab === 'RANDOM_QC' && <RandomQCTab />}
        {activeTab === 'FREQ_CHANGED' && <FrequentlyChangedTab />}
        {activeTab === 'KEYWORD_RULES' && <KeywordRulesTab />}
        {activeTab === 'VECTOR_CACHE' && <VectorCacheTab />}
        {activeTab === 'AUDIT_QUEUE' && (
            <div style={styles.emptyView}>
                <History size={48} opacity={0.1}/>
                <h3>Transaction Audit Queue</h3>
                <p>Advanced manual review dashboard coming in next update.</p>
            </div>
        )}
      </main>

      <style>{`@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }`}</style>
    </div>
  );
};

const styles = {
  container: { height: '100vh', display: 'flex', flexDirection: 'column', background: '#020617', color: '#e2e8f0', fontFamily: "'Inter', sans-serif" },
  header: { 
    padding: '1.25rem 2rem', 
    display: 'flex', 
    alignItems: 'center', 
    justifyContent: 'space-between', 
    background: 'rgba(15, 23, 42, 0.4)', 
    backdropFilter: 'blur(16px)', 
    borderBottom: '1px solid rgba(255, 255, 255, 0.05)',
    zIndex: 10
  },
  brand: { display: 'flex', alignItems: 'center', gap: '0.75rem' },
  brandIcon: { width: '40px', height: '40px', borderRadius: '12px', background: 'linear-gradient(135deg, #6366f1, #8b5cf6)', display: 'flex', alignItems: 'center', justifyContent: 'center' },
  brandName: { fontSize: '18px', fontWeight: 900, color: '#fff', letterSpacing: '-0.5px' },
  brandSub: { fontSize: '10px', color: '#94a3b8', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.5px' },
  
  statsOverview: { flexGrow: 1, margin: '0 3rem', display: 'flex', flexDirection: 'column', gap: '8px' },
  statBar: { height: '6px', width: '100%', background: 'rgba(148, 163, 184, 0.1)', borderRadius: '20px', overflow: 'hidden', display: 'flex' },
  statLegend: { display: 'flex', gap: '12px', flexWrap: 'wrap' },
  legendNode: { display: 'flex', alignItems: 'center', gap: '4px', fontSize: '10px', color: '#94a3b8' },
  dot: { width: '6px', height: '6px', borderRadius: '50%' },

  userProfile: { display: 'flex', alignItems: 'center', gap: '1rem' },
  userEmail: { fontSize: '12px', fontWeight: 700, color: '#312e81', background: '#38bdf8', padding: '4px 10px', borderRadius: '20px' },
  logoutBtn: { background: 'rgba(255, 255, 255, 0.05)', border: 'none', color: '#94a3b8', width: '32px', height: '32px', borderRadius: '50%', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', transition: 'all 0.2s', '&:hover': { background: '#ef4444', color: '#fff' } },

  navBar: { padding: '0.5rem 2rem', background: '#020617', borderBottom: '1px solid rgba(255, 255, 255, 0.03)', display: 'flex', gap: '0.5rem' },
  navLink: { background: 'transparent', border: 'none', color: '#64748b', display: 'flex', alignItems: 'center', gap: '8px', padding: '0.5rem 1rem', borderRadius: '8px', fontSize: '13px', fontWeight: 600, cursor: 'pointer', transition: 'all 0.2s' },
  navLinkActive: { background: 'rgba(99, 102, 241, 0.1)', border: 'none', color: '#fff', display: 'flex', alignItems: 'center', gap: '8px', padding: '0.5rem 1rem', borderRadius: '8px', fontSize: '13px', fontWeight: 700, cursor: 'pointer', boxShadow: '0 0 15px rgba(99, 102, 241, 0.1)' },
  
  workspace: { flexGrow: 1, padding: '1.5rem', minHeight: 0, overflow: 'hidden' },
  emptyView: { height: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', opacity: 0.3, textAlign: 'center', gap: '0.5rem' }
};

export default QCPanel;
