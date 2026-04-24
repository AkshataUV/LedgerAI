import React, { createContext, useContext, useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import API from '../api/api';

// const ParsingContext = createContext();
const ParsingContext = createContext();
export const useParsing = () => useContext(ParsingContext);

// Step definition logic — moved to context for consistency
export const extractionSteps = [
    { label: "Upload", icon: "FileUp", statuses: ["UPLOADED", "UPLOADING", "PROCESSING"] },
    { label: "Text Extraction", icon: "List", statuses: ["EXTRACTING_TEXT"] },
    { label: "Identification", icon: "Search", statuses: ["IDENTIFYING_FORMAT"] },
    { label: "Analysis", icon: "Cpu", statuses: ["PARSING_TRANSACTIONS", "PARSING_TRANSACTIONS_CODE"] },
    { label: "Ready", icon: "CheckCircle", statuses: ["AWAITING_REVIEW", "DONE", "APPROVE", "POSTED"] },
];

export const ParsingProvider = ({ children }) => {
    const [activeDoc, setActiveDoc] = useState(null);
    const [isExtracting, setIsExtracting] = useState(false);
    const [latestFinishedDocId, setLatestFinishedDocId] = useState(null);
    const [maxStepReached, setMaxStepReached] = useState(-1);

    const [notification, setNotification] = useState(null);

    const pollRef = useRef(null);
    const timerRef = useRef(null);

    useEffect(() => {
        return () => {
            if (pollRef.current) clearInterval(pollRef.current);
            if (timerRef.current) clearInterval(timerRef.current);
        };
    }, []);

    const startExtraction = async (file, password = "") => {
        setIsExtracting(true);
        setMaxStepReached(0); // Start at step 0
        setActiveDoc({
            id: null,
            name: file.name,
            status: "UPLOADING",
            processingStatus: "UPLOADING",
            elapsedSeconds: 0,
            parsedType: null
        });

        const formData = new FormData();
        formData.append("file", file);
        if (password) formData.append("password", password);

        try {
            const res = await API.post("/documents/upload", formData);
            const docId = res.data.document_id;
            setActiveDoc(prev => ({ ...prev, id: docId, status: "PROCESSING" }));

            if (timerRef.current) clearInterval(timerRef.current);
            timerRef.current = setInterval(() => {
                setActiveDoc(prev => prev ? { ...prev, elapsedSeconds: prev.elapsedSeconds + 1 } : null);
            }, 1000);

            startPolling(docId, file.name);

        } catch (err) {
            setIsExtracting(false);
            setActiveDoc(null);
            setMaxStepReached(-1);
            throw err;
        }
    };

    const startPolling = (docId, fileName) => {
        if (pollRef.current) clearInterval(pollRef.current);
        pollRef.current = setInterval(async () => {
            try {
                const statusRes = await API.get(`/documents/status/${docId}`);
                const { status: docStatus, transaction_parsed_type: docParsedType } = statusRes.data;

                setActiveDoc(prev => {
                    const newDoc = {
                        ...prev,
                        processingStatus: docStatus,
                        parsedType: docParsedType || prev?.parsedType
                    };

                    // Update global progress gate
                    const stepIdx = extractionSteps.findIndex(s => s.statuses.includes(docStatus));
                    if (stepIdx > maxStepReached) {
                        setMaxStepReached(stepIdx);
                    }

                    return newDoc;
                });

                if (["AWAITING_REVIEW", "APPROVE", "POSTED", "DONE"].includes(docStatus)) {
                    stopPolling(docId, "DONE");
                    setNotification({
                        type: 'success',
                        title: 'Extraction Complete',
                        message: `Transactions for "${fileName}" have been extracted successfully.`,
                        docId: docId
                    });
                } else if (docStatus === "FAILED") {
                    stopPolling(docId, "ERROR");
                    setNotification({
                        type: 'error',
                        title: 'Extraction Failed',
                        message: `Failed to process "${fileName}". Please check the file if it's protected or corrupted.`,
                        docId: docId
                    });
                }
            } catch (err) {
                console.error("Polling error", err);
            }
        }, 2000);
    };

    const retryExtraction = (docId, fileName) => {
        setIsExtracting(true);
        setMaxStepReached(0);
        setActiveDoc({
            id: docId,
            name: fileName,
            status: "PROCESSING",
            processingStatus: "UPLOADED",
            elapsedSeconds: 0,
            parsedType: null
        });

        if (timerRef.current) clearInterval(timerRef.current);
        timerRef.current = setInterval(() => {
            setActiveDoc(prev => prev ? { ...prev, elapsedSeconds: prev.elapsedSeconds + 1 } : null);
        }, 1000);

        startPolling(docId, fileName);
    };

    const stopPolling = (docId, finalStatus) => {
        if (pollRef.current) clearInterval(pollRef.current);
        if (timerRef.current) clearInterval(timerRef.current);
        setIsExtracting(false);
        setLatestFinishedDocId(docId);
        setActiveDoc(prev => prev ? { ...prev, status: finalStatus } : null);
    };

    const clearActiveDoc = () => {
        setActiveDoc(null);
        setIsExtracting(false);
        setMaxStepReached(-1);
    };

    return (
        <ParsingContext.Provider value={{
            activeDoc,
            isExtracting,
            latestFinishedDocId,
            startExtraction,
            retryExtraction,
            clearActiveDoc,
            setLatestFinishedDocId,
            notification,
            setNotification,
            maxStepReached
        }}>
            {children}
            <NotificationPortal notification={notification} onClose={() => setNotification(null)} />
        </ParsingContext.Provider>
    );
};

const NotificationPortal = ({ notification, onClose }) => {
    const navigate = useNavigate();
    
    useEffect(() => {
        if (!notification) return;
        
        // Auto-dismiss after 4 seconds
        const timer = setTimeout(() => {
            onClose();
        }, 4000);
        
        return () => clearTimeout(timer);
    }, [notification, onClose]);

    if (!notification) return null;

    const handleAction = () => {
        if (notification.type === 'success' && notification.docId) {
            navigate(`/review/${notification.docId}`);
        }
        onClose();
    };

    return (
        <div 
            onClick={handleAction}
            style={{
                position: 'fixed',
                top: '24px',
                right: '24px',
                zIndex: 9999,
                width: '360px',
                animation: 'slideIn 0.4s ease-out',
                cursor: 'pointer'
            }}
        >
            <div style={{
                background: 'var(--bg-secondary, rgba(255, 255, 255, 0.95))',
                backdropFilter: 'blur(12px)',
                borderRadius: '16px',
                border: `1px solid ${notification.type === 'success' ? '#27ae6033' : '#e74c3c33'}`,
                boxShadow: '0 10px 40px rgba(0,0,0,0.1)',
                padding: '1.25rem',
                display: 'flex',
                gap: '1rem',
                alignItems: 'center'
            }}>
                <div style={{
                    width: '40px',
                    height: '40px',
                    borderRadius: '50%',
                    background: notification.type === 'success' ? '#def7ec' : '#fde8e8',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    flexShrink: 0
                }}>
                    <span style={{ color: notification.type === 'success' ? '#03543f' : '#9b1c1c', fontSize: '1.2rem', fontWeight: 800 }}>
                        {notification.type === 'success' ? '✓' : '!'}
                    </span>
                </div>
                <div style={{ flex: 1 }}>
                    <div style={{ fontWeight: 800, fontSize: '0.95rem', color: 'var(--text-primary, #1e1b4b)', marginBottom: '2px' }}>
                        {notification.title}
                    </div>
                    <div style={{ fontSize: '0.8rem', color: 'var(--text-secondary, #6b7280)', lineHeight: 1.4 }}>
                        {notification.message}
                    </div>
                </div>
            </div>
            <style>{`
                @keyframes slideIn {
                    from { transform: translateX(100%); opacity: 0; }
                    to { transform: translateX(0); opacity: 1; }
                }
            `}</style>
        </div>
    );
};