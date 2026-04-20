const supabase = require('../config/supabaseClient'); 
require('dotenv').config();
const axios = require('axios');

const ML_SERVICE_URL = process.env.ML_SERVICE_URL || `http://127.0.0.1:${process.env.PYTHON_PORT || 5000}`;

/**
 * QC Status Dashboard Endpoints
 */

const getReviewDocuments = async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('documents')
            .select(`
                document_id, user_id, status, file_name, transaction_parsed_type,
                statement_categories!inner(statement_id, statement_type, institution_name, status)
            `)
            .eq('statement_categories.status', 'EXPERIMENTAL');

        if (error) throw error;
        
        // Map to the format LEDGER_AI expects
        const mapped = data.map(d => ({
            document_id: d.document_id,
            user_id: d.user_id,
            doc_status: d.status,
            file_name: d.file_name,
            transaction_parsed_type: d.transaction_parsed_type,
            statement_id: d.statement_categories.statement_id,
            statement_type: d.statement_categories.statement_type,
            institution_name: d.statement_categories.institution_name,
            format_status: d.statement_categories.status
        }));

        return res.status(200).json(mapped);
    } catch (err) {
        console.error('❌ getReviewDocuments error:', err);
        return res.status(500).json({ error: 'Failed to fetch review documents' });
    }
};

const getRandomQCResults = async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('random_qc_results')
            .select('*')
            .order('created_at', { ascending: false });

        if (error) throw error;
        return res.status(200).json(data);
    } catch (err) {
        console.error('❌ getRandomQCResults error:', err);
        return res.status(500).json({ error: 'Failed to fetch random QC results' });
    }
};

const getFrequentlyChangedDocs = async (req, res) => {
    try {
        // Documents with lots of overrides/manual corrections or FLAGGED status
        const { data, error } = await supabase
            .from('random_qc_results')
            .select('*')
            .eq('qc_status', 'FLAGGED')
            .order('created_at', { ascending: false });

        if (error) throw error;
        return res.status(200).json(data);
    } catch (err) {
        console.error('❌ getFrequentlyChangedDocs error:', err);
        return res.status(500).json({ error: 'Failed to fetch frequently changed documents' });
    }
};

// ─── COA Modules Management ───────────────────────────────────────────────────

const deleteModule = async (req, res) => {
    try {
        const { id } = req.params;
        const { error } = await supabase
            .from('coa_modules')
            .delete()
            .eq('module_id', id);

        if (error) throw error;
        return res.status(200).json({ success: true, message: 'Module deleted' });
    } catch (err) {
        console.error('❌ deleteModule error:', err);
        return res.status(500).json({ error: 'Failed to delete module' });
    }
};

// ─── Global Keyword Rules Management ──────────────────────────────────────────

const getGlobalKeywordRules = async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('global_keyword_rules')
            .select('*')
            .order('keyword', { ascending: true });

        if (error) throw error;
        return res.status(200).json(data);
    } catch (err) {
        console.error('❌ getGlobalKeywordRules error:', err);
        return res.status(500).json({ error: 'Failed to fetch global keyword rules' });
    }
};

const createKeywordRule = async (req, res) => {
    try {
        const { keyword, target_template_id, match_type, priority, is_active } = req.body;
        
        if (!keyword) return res.status(400).json({ error: 'Keyword is required' });

        const { data, error } = await supabase
            .from('global_keyword_rules')
            .insert([
                { 
                    keyword: keyword.trim().toUpperCase(), 
                    target_template_id, 
                    match_type: match_type || 'CONTAINS', 
                    priority: priority || 90, 
                    is_active: is_active !== undefined ? is_active : true,
                    hit_count: 0
                }
            ])
            .select()
            .single();

        if (error) throw error;
        return res.status(201).json(data);
    } catch (err) {
        console.error('❌ createKeywordRule error:', err);
        return res.status(500).json({ error: 'Failed to create keyword rule' });
    }
};

const bulkCreateKeywordRules = async (req, res) => {
    try {
        const { rules } = req.body; // Array of { keyword, target_template_id }
        if (!rules || !Array.isArray(rules)) return res.status(400).json({ error: 'Rules array is required' });

        const formatted = rules.map(r => ({
            keyword: r.keyword.trim().toUpperCase(),
            target_template_id: r.target_template_id,
            match_type: 'CONTAINS',
            priority: 90,
            is_active: true
        }));

        const { data, error } = await supabase
            .from('global_keyword_rules')
            .upsert(formatted, { onConflict: 'keyword' })
            .select();

        if (error) throw error;
        return res.status(201).json({ success: true, count: data.length });
    } catch (err) {
        console.error('❌ bulkCreateKeywordRules error:', err);
        return res.status(500).json({ error: 'Failed to bulk create keyword rules' });
    }
};

const deleteKeywordRule = async (req, res) => {
    try {
        const { id } = req.params;
        const { error } = await supabase
            .from('global_keyword_rules')
            .delete()
            .eq('keyword_id', id);

        if (error) throw error;
        return res.status(200).json({ success: true, message: 'Keyword rule deleted' });
    } catch (err) {
        console.error('❌ deleteKeywordRule error:', err);
        return res.status(500).json({ error: 'Failed to delete keyword rule' });
    }
};

// ─── Global Vector Cache Management ───────────────────────────────────────────

const getGlobalVectorCache = async (req, res) => {
    try {
        const { data, error } = await supabase
            .from('global_vector_cache')
            .select('cache_id, clean_name, target_template_id, approval_count, is_verified, created_at, updated_at')
            .order('cache_id', { ascending: true });

        if (error) throw error;
        return res.status(200).json(data);
    } catch (err) {
        console.error('❌ getGlobalVectorCache error:', err);
        return res.status(500).json({ error: 'Failed to fetch global vector cache' });
    }
};

const createVectorCacheEntry = async (req, res) => {
    try {
        const { clean_name, target_template_id } = req.body;
        if (!clean_name) return res.status(400).json({ error: 'clean_name is required.' });

        const uppercaseName = clean_name.trim().toUpperCase();

        // 1. Check for duplicates
        const { data: existing } = await supabase
            .from('global_vector_cache')
            .select('cache_id')
            .eq('clean_name', uppercaseName)
            .maybeSingle();

        if (existing) {
            return res.status(409).json({ error: `"${uppercaseName}" already exists in the vector cache.` });
        }

        // 2. Generate embedding via ML service
        const embedRes = await axios.post(`${ML_SERVICE_URL}/embed`, { text: uppercaseName });
        const { embedding } = embedRes.data;

        if (!embedding || !Array.isArray(embedding)) {
            throw new Error('Failed to retrieve 384-dimensional array embedding');
        }

        // 3. Insert into DB
        const { data, error } = await supabase
            .from('global_vector_cache')
            .insert([
                {
                    clean_name: uppercaseName,
                    target_template_id: target_template_id || null,
                    embedding: embedding,
                    approval_count: 100, // Pre-approved for manual addition
                    is_verified: true
                }
            ])
            .select('cache_id, clean_name, target_template_id, approval_count, is_verified, created_at, updated_at')
            .single();

        if (error) throw error;
        return res.status(201).json(data);

    } catch (err) {
        console.error('❌ createVectorCacheEntry error:', err);
        return res.status(500).json({ error: 'Failed to create vector cache entry: ' + err.message });
    }
};

const bulkCreateVectorCacheEntries = async (req, res) => {
    try {
        const { entries } = req.body; // Array of { name, target_template_id }
        if (!entries || !Array.isArray(entries)) return res.status(400).json({ error: 'Entries array is required' });

        const results = [];
        for (const entry of entries) {
            try {
                const uppercaseName = entry.name.trim().toUpperCase();
                
                // Embed
                const embedRes = await axios.post(`${ML_SERVICE_URL}/embed`, { text: uppercaseName });
                const { embedding } = embedRes.data;

                results.push({
                    clean_name: uppercaseName,
                    target_template_id: entry.target_template_id || null,
                    embedding,
                    approval_count: 100,
                    is_verified: true
                });
            } catch (e) {
                console.error(`Skipping entry ${entry.name}: ${e.message}`);
            }
        }

        const { data, error } = await supabase
            .from('global_vector_cache')
            .upsert(results, { onConflict: 'clean_name' })
            .select();

        if (error) throw error;
        return res.status(201).json({ success: true, count: data.length });
    } catch (err) {
        console.error('❌ bulkCreateVectorCacheEntries error:', err);
        return res.status(500).json({ error: 'Failed to bulk create vector cache entries' });
    }
};

const deleteVectorCacheEntry = async (req, res) => {
    try {
        const { id } = req.params;
        const { error } = await supabase
            .from('global_vector_cache')
            .delete()
            .eq('cache_id', id);

        if (error) throw error;
        return res.status(200).json({ success: true, message: 'Vector cache entry deleted' });
    } catch (err) {
        console.error('❌ deleteVectorCacheEntry error:', err);
        return res.status(500).json({ error: 'Failed to delete vector cache entry' });
    }
};

module.exports = {
    getReviewDocuments,
    getRandomQCResults,
    getFrequentlyChangedDocs,
    deleteModule,
    getGlobalKeywordRules,
    createKeywordRule,
    bulkCreateKeywordRules,
    deleteKeywordRule,
    getGlobalVectorCache,
    createVectorCacheEntry,
    bulkCreateVectorCacheEntries,
    deleteVectorCacheEntry
};
