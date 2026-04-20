import React, { useState, useEffect, useRef } from 'react';
import { 
  Bot, X, Send, Minus, Maximize2, 
  TrendingUp, AlertCircle, HelpCircle, Sparkles, MessageCircle
} from 'lucide-react';
import { supabase } from '../../shared/supabase';
import './chatbot.css';

/**
 * LedgerBuddy AI Financial Assistant
 * A floating AI chatbot that provides financial insights based on user data.
 */
const LedgerBuddy = ({ user, isDarkMode }) => {
  const [isOpen, setIsOpen] = useState(false);
  const [isMinimized, setIsMinimized] = useState(false);
  const [input, setInput] = useState('');
  const [messages, setMessages] = useState([
    { 
      id: 'welcome', 
      type: 'bot', 
      text: `Hi ${user?.email?.split('@')[0] || 'there'}! I'm LedgerBuddy, your AI financial assistant. How can I help you understand your spending today?`,
      timestamp: new Date()
    }
  ]);
  const [loading, setLoading] = useState(false);
  const scrollRef = useRef(null);

  // Load history on mount
  useEffect(() => {
    const fetchHistory = async () => {
      try {
        const { data: { session } } = await supabase.auth.getSession();
        if (!session) return;
        
        const res = await fetch('/api/chat/history', {
          headers: { 'Authorization': `Bearer ${session.access_token}` }
        });
        
        if (res.ok) {
          const data = await res.json();
          if (data && data.length > 0) {
            setMessages(data.map(m => ({ ...m, timestamp: new Date(m.timestamp) })));
          }
        }
      } catch (err) {
        console.error('Failed to load chat history:', err);
      }
    };
    fetchHistory();
  }, []);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, isOpen]);

  const toggleChat = () => {
    setIsOpen(!isOpen);
    setIsMinimized(false);
  };

  const handleSend = async (e) => {
    if (e) e.preventDefault();
    if (!input.trim() || loading) return;

    const userMessage = {
      id: Date.now(),
      type: 'user',
      text: input,
      timestamp: new Date()
    };

    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setLoading(true);

    try {
      const { data: { session } } = await supabase.auth.getSession();
      
      // API call to LedgerBuddy backend
      const response = await fetch('/api/chat/message', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${session?.access_token}`
        },
        body: JSON.stringify({ message: input })
      });

      const data = await response.json();
      
      if (!response.ok) throw new Error(data.error || 'Server error');
      
      const botResponse = {
        id: Date.now() + 1,
        type: 'bot',
        text: data.text,
        timestamp: new Date()
      };
      
      setMessages(prev => [...prev, botResponse]);
      setLoading(false);
    } catch (err) {
      console.error('Chat failed:', err);
      const errorMsg = {
        id: Date.now() + 1,
        type: 'bot',
        text: `Error: ${err.message}. I'm having trouble processing that right now.`,
        timestamp: new Date()
      };
      setMessages(prev => [...prev, errorMsg]);
      setLoading(false);
    }
  };

  const quickActions = [
    { label: "Why did I overspend?", icon: <TrendingUp size={12}/> },
    { label: "Anomalies this week", icon: <AlertCircle size={12}/> },
    { label: "Safe to spend?", icon: <Sparkles size={12}/> }
  ];

  if (!isOpen) {
    return (
      <button className="ledgerbuddy-trigger" onClick={toggleChat} title="Ask LedgerBuddy">
        <div className="trigger-pulse"></div>
        <Bot size={24} color="#fff" />
        <span className="trigger-label">LedgerBuddy</span>
      </button>
    );
  }

  return (
    <div className={`ledgerbuddy-panel ${isMinimized ? 'minimized' : ''}`}>
      {/* Header */}
      <header className="chat-header">
        <div className="header-info">
          <div className="bot-avatar"><Bot size={16} color="#fff"/></div>
          <div className="bot-meta">
            <span className="bot-name">LedgerBuddy</span>
            <span className="bot-status">Online • AI Assistant</span>
          </div>
        </div>
        <div className="header-actions">
          <button onClick={() => setIsMinimized(!isMinimized)} title={isMinimized ? "Expand" : "Minimize"}>
            {isMinimized ? <Maximize2 size={16}/> : <Minus size={16}/>}
          </button>
          <button onClick={toggleChat} title="Close"><X size={16}/></button>
        </div>
      </header>

      {!isMinimized && (
        <>
          {/* Chat area */}
          <div className="chat-messages" ref={scrollRef}>
            {messages.map((msg) => (
              <div key={msg.id} className={`message-row ${msg.type}`}>
                {msg.type === 'bot' && <div className="msg-avatar"><Bot size={12}/></div>}
                <div className="message-bubble">
                  {msg.text}
                  <div className="msg-time">
                    {msg.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </div>
                </div>
              </div>
            ))}
            {loading && (
              <div className="message-row bot">
                <div className="msg-avatar"><Bot size={12}/></div>
                <div className="message-bubble typing">
                  <div className="dot"></div><div className="dot"></div><div className="dot"></div>
                </div>
              </div>
            )}
          </div>

          {/* Prompt options */}
          {messages.length < 3 && !loading && (
            <div className="quick-actions">
              {quickActions.map((action, idx) => (
                <button key={idx} className="action-tag" onClick={() => {
                  setInput(action.label);
                  // Focus input somehow or just let user send
                }}>
                  {action.icon}
                  {action.label}
                </button>
              ))}
            </div>
          )}

          {/* Input area */}
          <form className="chat-input-area" onSubmit={handleSend}>
            <input 
              type="text" 
              placeholder="Ask anything about your finances..." 
              value={input}
              onChange={(e) => setInput(e.target.value)}
              disabled={loading}
            />
            <button type="submit" className="send-btn" disabled={!input.trim() || loading}>
              <Send size={18}/>
            </button>
          </form>
        </>
      )}
    </div>
  );
};

export default LedgerBuddy;
