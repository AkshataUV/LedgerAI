import React, { useState, useEffect } from 'react';
import { supabase } from '../shared/supabase';
import { 
  Package, Search, Trash2, Package as ModuleIcon, 
  ChevronRight, AlertCircle, Loader2
} from 'lucide-react';
import axios from 'axios';

const BADGE_COLORS = {
  'CORE': { bg: 'rgba(56, 189, 248, 0.1)', text: '#38bdf8', border: 'rgba(56, 189, 248, 0.3)' },
  'INDIVIDUAL': { bg: 'rgba(52, 211, 153, 0.1)', text: '#34d399', border: 'rgba(52, 211, 153, 0.3)' },
  'BUSINESS': { bg: 'rgba(251, 191, 36, 0.1)', text: '#fbbf24', border: 'rgba(251, 191, 36, 0.3)' }
};

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:3000';

const CoaLibraryTab = () => {
  const [modules, setModules] = useState([]);
  const [selectedModule, setSelectedModule] = useState(null);
  const [templates, setTemplates] = useState([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(true);

  const fetchModules = async () => {
    setLoading(true);
    try {
      const { data: coaModules, error: mError } = await supabase.from('coa_modules').select('*');
      if (mError) throw mError;
      
      const { data: tmpData } = await supabase.from('coa_templates').select('module_id');
      const counts = (tmpData || []).reduce((acc, curr) => {
        acc[curr.module_id] = (acc[curr.module_id] || 0) + 1;
        return acc;
      }, {});

      const categoryOrder = { 'CORE': 1, 'INDIVIDUAL': 2, 'BUSINESS': 3 };
      const sorted = (coaModules || []).sort((a, b) => {
        const orderA = categoryOrder[a.category] || 99;
        const orderB = categoryOrder[b.category] || 99;
        if (orderA !== orderB) return orderA - orderB;
        return a.module_name.localeCompare(b.module_name);
      }).map(m => ({ ...m, accountCount: counts[m.module_id] || 0 }));

      setModules(sorted);
      if (sorted.length > 0 && !selectedModule) setSelectedModule(sorted[0]);
    } catch (err) {
      console.error('Failed to fetch COA Modules:', err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchModules(); }, []);

  useEffect(() => {
    if (selectedModule) {
      const fetchTemplates = async () => {
        try {
          const coreIds = modules.filter(m => m.category === 'CORE').map(m => m.module_id);
          const idsToFetch = [...new Set([selectedModule.module_id, ...coreIds])];
          const { data, error } = await supabase.from('coa_templates').select('*').in('module_id', idsToFetch);
          if (error) throw error;

          const buildHierarchy = (items) => {
            const map = {};
            items.forEach(item => { map[item.template_id] = { ...item, children: [] }; });
            const roots = [];
            items.forEach(item => {
              if (item.parent_template_id && map[item.parent_template_id]) {
                map[item.parent_template_id].children.push(map[item.template_id]);
              } else {
                roots.push(map[item.template_id]);
              }
            });
            return roots;
          };

          const sortTree = (nodes) => {
            return nodes.sort((a, b) => a.account_name.localeCompare(b.account_name)).map(n => ({
              ...n, children: sortTree(n.children)
            }));
          };

          setTemplates(sortTree(buildHierarchy(data || [])));
        } catch (err) {
          console.error('Failed to fetch Templates:', err);
        }
      };
      fetchTemplates();
    }
  }, [selectedModule, modules]);

  const handleDeleteModule = async (id, name) => {
    if (!window.confirm(`Are you sure you want to delete the module "${name}"? This will delete all associated templates.`)) return;
    try {
        const { data: { session } } = await supabase.auth.getSession();
        await axios.delete(`${API_BASE_URL}/api/qc/modules/${id}`, {
            headers: { Authorization: `Bearer ${session?.access_token}` }
        });
        fetchModules();
        if (selectedModule?.module_id === id) setSelectedModule(null);
    } catch (err) {
        alert('Deletion failed: ' + (err.response?.data?.error || err.message));
    }
  };

  const renderTemplateRow = (tmp, depth = 0) => {
    const isCoreTemplate = modules.some(m => m.module_id === tmp.module_id && m.category === 'CORE');
    return (
      <React.Fragment key={tmp.template_id}>
        <div style={{ ...styles.gridRow, paddingLeft: `${1.5 + depth * 1.5}rem` }}>
          <div style={styles.nameCell}>
            {depth > 0 && <span style={styles.branch}>└─</span>}
            <span style={styles.accountName}>{tmp.account_name}</span>
            {isCoreTemplate && <span style={styles.coreTag}>CORE</span>}
          </div>
          <div style={styles.typeCell}>{tmp.account_type || '—'}</div>
          <div style={styles.natureCell}>
            <span style={{ 
               color: tmp.balance_nature === 'DEBIT' ? '#38bdf8' : '#fbbf24',
               fontSize: '10px', fontWeight: 800, letterSpacing: '0.5px'
            }}>
                {tmp.balance_nature}
            </span>
          </div>
        </div>
        {tmp.children && tmp.children.map(child => renderTemplateRow(child, depth + 1))}
      </React.Fragment>
    );
  };

  return (
    <div style={styles.tabLayout}>
      <aside style={styles.sidebar}>
        <div style={styles.searchBox}>
          <Search size={14} style={styles.searchIcon} />
          <input 
            type="text" 
            placeholder="Search modules..." 
            style={styles.searchInput} 
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
          />
        </div>
        <div style={styles.moduleList}>
          {loading ? <div style={styles.loadingSide}>Loading Modules...</div> : 
           modules
            .filter(mod => mod.module_name.toUpperCase().includes(searchQuery.toUpperCase()))
            .map(mod => (
              <div 
                key={mod.module_id} 
                onClick={() => setSelectedModule(mod)}
                style={selectedModule?.module_id === mod.module_id ? styles.cardActive : styles.card}
              >
                <div style={styles.cardHeader}>
                   <div style={styles.cardTitle}>{mod.module_name}</div>
                   <div style={{
                     ...styles.badge,
                     color: BADGE_COLORS[mod.category]?.text,
                     backgroundColor: BADGE_COLORS[mod.category]?.bg,
                     borderColor: BADGE_COLORS[mod.category]?.border
                   }}>
                     {mod.category}
                   </div>
                </div>
                <div style={styles.cardFooter}>
                    <span>{mod.accountCount} Linked</span>
                    <button 
                      style={styles.deleteBtn} 
                      onClick={(e) => { e.stopPropagation(); handleDeleteModule(mod.module_id, mod.module_name); }}
                    >
                      <Trash2 size={12}/>
                    </button>
                </div>
              </div>
            ))
          }
        </div>
      </aside>

      <section style={styles.content}>
        {selectedModule ? (
          <div style={styles.tableCard}>
            <header style={styles.contentHeader}>
               <div>
                  <h2 style={styles.contentTitle}>{selectedModule.module_name}</h2>
                  <p style={styles.contentSub}>{selectedModule.description || 'System standard COA library module'}</p>
               </div>
            </header>
            <div style={styles.tableWrapper}>
               <div style={styles.tableHead}>
                 <div style={{ flex: 2 }}>ACCOUNT NAME</div>
                 <div style={{ flex: 1 }}>TYPE</div>
                 <div style={{ flex: 1 }}>NATURE</div>
               </div>
               <div style={styles.tableBody}>
                 {templates.length === 0 ? (
                    <div style={styles.emptyTable}>No templates in this hierarchy.</div>
                 ) : (
                    templates.map(tmp => renderTemplateRow(tmp))
                 )}
               </div>
            </div>
          </div>
        ) : (
          <div style={styles.emptyState}>
            <Package size={48} opacity={0.1} />
            <p>Select a module from the left to view Chart of Accounts library</p>
          </div>
        )}
      </section>
    </div>
  );
};

const styles = {
  tabLayout: { display: 'flex', height: '100%', gap: '1.5rem', minWidth: 0 },
  sidebar: { width: '320px', display: 'flex', flexDirection: 'column', gap: '1rem', flexShrink: 0 },
  searchBox: { display: 'flex', alignItems: 'center', gap: '8px', background: 'rgba(0,0,0,0.3)', border: '1px solid rgba(255,255,255,0.08)', borderRadius: '12px', padding: '0.6rem 1rem' },
  searchIcon: { opacity: 0.3 },
  searchInput: { background: 'transparent', border: 'none', color: '#fff', fontSize: '13px', outline: 'none', width: '100%' },
  moduleList: { flexGrow: 1, overflowY: 'auto', display: 'flex', flexDirection: 'column', gap: '0.75rem' },
  
  card: { padding: '1.25rem', borderRadius: '16px', background: 'rgba(30, 41, 59, 0.4)', border: '1px solid rgba(255,255,255,0.04)', cursor: 'pointer', transition: 'all 0.2s' },
  cardActive: { padding: '1.25rem', borderRadius: '16px', background: 'rgba(99, 102, 241, 0.1)', border: '1px solid #6366f1', cursor: 'pointer', transition: 'all 0.2s', boxShadow: '0 0 20px rgba(99, 102, 241, 0.1)' },
  cardHeader: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '8px' },
  cardTitle: { fontSize: '14px', fontWeight: 800, color: '#fff' },
  cardFooter: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '10px', color: '#64748b', fontWeight: 600 },
  badge: { fontSize: '9px', fontWeight: 900, padding: '2px 8px', borderRadius: '20px', border: '1px solid' },
  deleteBtn: { background: 'transparent', border: 'none', color: '#64748b', cursor: 'pointer', '&:hover': { color: '#ef4444' } },

  content: { flexGrow: 1, minWidth: 0 },
  tableCard: { height: '100%', background: 'rgba(15, 23, 42, 0.3)', border: '1px solid rgba(255,255,255,0.08)', borderRadius: '20px', display: 'flex', flexDirection: 'column', overflow: 'hidden' },
  contentHeader: { padding: '1.5rem', borderBottom: '1px solid rgba(255,255,255,0.05)' },
  contentTitle: { fontSize: '1.5rem', fontWeight: 800, color: '#fff', margin: 0 },
  contentSub: { fontSize: '12px', opacity: 0.5, margin: '4px 0 0 0' },
  
  tableWrapper: { flexGrow: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' },
  tableHead: { display: 'flex', padding: '0.85rem 1.5rem', background: 'rgba(0,0,0,0.3)', color: '#94a3b8', fontSize: '10px', fontWeight: 900, letterSpacing: '1px' },
  tableBody: { flexGrow: 1, overflowY: 'auto' },
  gridRow: { display: 'flex', padding: '0.85rem 1.5rem', borderBottom: '1px solid rgba(255,255,255,0.03)', fontSize: '13px', alignItems: 'center' },
  nameCell: { flex: 2, display: 'flex', alignItems: 'center', gap: '8px' },
  typeCell: { flex: 1, opacity: 0.6, fontSize: '12px' },
  natureCell: { flex: 1 },
  branch: { color: '#64748b', opacity: 0.4 },
  accountName: { color: '#e2e8f0', fontWeight: 600 },
  coreTag: { fontSize: '8px', fontWeight: 900, background: 'rgba(56, 189, 248, 0.1)', color: '#38bdf8', padding: '2px 6px', borderRadius: '4px', border: '1px solid rgba(56, 189, 248, 0.2)' },
  
  loadingSide: { padding: '2rem', textAlign: 'center', color: '#64748b', fontSize: '12px' },
  emptyTable: { padding: '4rem', textAlign: 'center', color: '#64748b', opacity: 0.5 },
  emptyState: { height: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', color: '#64748b', textAlign: 'center', padding: '2rem', gap: '1rem' }
};

export default CoaLibraryTab;
