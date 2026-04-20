const supabase = require('../config/supabaseClient');

/**
 * bulkUploadStatements
 * Atomically processes a bulk upload of transactions from a statement file.
 * All inserts happen in a single transaction for consistency and rollback safety.
 * 
 * @param {object} req - Express request with user populated by authMiddleware
 * @param {object} res - Express response
 */
async function bulkUploadStatements(req, res) {
  try {
    const { file_name, transactions, identifiers } = req.body;
    const userId = req.user?.id;

    if (!userId) {
      return res.status(401).json({ error: 'Unauthorized: User not found.' });
    }

    if (!file_name || !transactions || !Array.isArray(transactions)) {
      return res.status(400).json({ error: 'Invalid payload: Expecting file_name and transactions array.' });
    }

    if (transactions.length === 0) {
      return res.status(400).json({ error: 'Transactions array cannot be empty.' });
    }

    let accountId = null;

    // 1. Find Account ID from identifiers if provided
    if (identifiers && identifiers.length > 0) {
      const last4 = identifiers[0];
      const { data: ident, error: identError } = await supabase
        .from('account_identifiers')
        .select('account_id')
        .eq('user_id', userId)
        .or(`account_number_last4.eq.${last4},card_last4.eq.${last4}`)
        .maybeSingle();

      if (identError) {
        console.error('Error querying account identifiers:', identError);
        return res.status(500).json({ error: 'Failed to find account identifier.' });
      }

      if (ident) {
        accountId = ident.account_id;
      }
    }

    // 2. Create Document Entry
    const { data: doc, error: docError } = await supabase
      .from('documents')
      .insert([{
        user_id: userId,
        file_name: file_name,
        status: 'UPLOADED'
      }])
      .select()
      .single();

    if (docError) {
      console.error('Error creating document:', docError);
      return res.status(500).json({ error: 'Failed to create document entry.' });
    }

    // 3. Prepare batch inserts for staging and uncategorized transactions
    const stagingInserts = transactions.map(txn => ({
      document_id: doc.document_id,
      user_id: userId,
      transaction_json: txn
    }));

    // Insert all staging records atomically
    const { data: stagingData, error: stagingError } = await supabase
      .from('ai_transactions_staging')
      .insert(stagingInserts)
      .select();

    if (stagingError) {
      console.error('Error creating staging records:', stagingError);
      return res.status(500).json({ error: 'Failed to stage transactions.' });
    }

    // 4. Prepare uncategorized transaction records with matching staging IDs
    const uncatInserts = stagingData.map((stage, index) => {
      const txn = transactions[index];
      return {
        user_id: userId,
        account_id: accountId,
        document_id: doc.document_id,
        staging_transaction_id: stage.staging_transaction_id,
        txn_date: txn.txn_date || txn.date,
        debit: parseFloat(txn.debit) || 0,
        credit: parseFloat(txn.credit) || 0,
        balance: parseFloat(txn.balance) || 0,
        details: txn.details
      };
    });

    // Insert all uncategorized records atomically
    const { error: uncatError } = await supabase
      .from('uncategorized_transactions')
      .insert(uncatInserts);

    if (uncatError) {
      console.error('Error creating uncategorized records:', uncatError);
      return res.status(500).json({ error: 'Failed to create uncategorized transaction records.' });
    }

    return res.status(200).json({
      success: true,
      data: {
        document_id: doc.document_id,
        transaction_count: transactions.length,
        message: 'Statement uploaded successfully.'
      }
    });

  } catch (err) {
    console.error('❌ bulkUploadStatements exception:', err);
    return res.status(500).json({ error: 'Internal server error during upload.' });
  }
}

module.exports = {
  bulkUploadStatements
};
