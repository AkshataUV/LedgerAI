const logger = require('../utils/logger');
require('dotenv').config();

// LLM Provider Configuration
const LLM_PROVIDER = process.env.LLM_PROVIDER || 'openrouter'; // 'openrouter' or 'google'
const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY;
const LLM_MODEL = process.env.LLM_MODEL || 'google/gemini-2.0-flash-exp';

// Provider-specific configurations
const PROVIDERS = {
  openrouter: {
    url: 'https://openrouter.ai/api/v1/chat/completions',
    defaultModel: 'google/gemini-2.0-flash-exp',
    getHeaders: (apiKey) => ({
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
      'HTTP-Referer': 'http://localhost:3000',
      'X-Title': 'LedgerAI v2.0'
    }),
    formatRequest: (model, messages, temperature) => ({
      model,
      messages,
      temperature
    }),
    extractResponse: (data) => data.choices?.[0]?.message?.content?.trim()
  },
  google: {
    url: (model, apiKey) => `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${apiKey}`,
    defaultModel: 'gemini-2.0-flash-exp',
    getHeaders: () => ({
      'Content-Type': 'application/json'
    }),
    formatRequest: (model, messages, temperature) => {
      // Convert OpenAI-style messages to Google's format
      const contents = messages.map(msg => ({
        role: msg.role === 'system' ? 'user' : msg.role, // Google doesn't have 'system' role
        parts: [{ text: msg.content }]
      }));

      // Merge system message with first user message if present
      if (messages[0]?.role === 'system' && messages.length > 1) {
        const systemContent = messages[0].content;
        const userContent = messages[1].content;
        return {
          contents: [
            {
              role: 'user',
              parts: [{ text: `${systemContent}\n\n${userContent}` }]
            },
            ...messages.slice(2).map(msg => ({
              role: msg.role === 'assistant' ? 'model' : 'user',
              parts: [{ text: msg.content }]
            }))
          ],
          generationConfig: {
            temperature: temperature,
            maxOutputTokens: 32768
          }
        };
      }

      return {
        contents: contents.map(c => ({
          ...c,
          role: c.role === 'assistant' ? 'model' : 'user'
        })),
        generationConfig: {
          temperature: temperature,
          maxOutputTokens: 8192
        }
      };
    },
    extractResponse: (data) => data.candidates?.[0]?.content?.parts?.[0]?.text?.trim()
  }
};

/**
 * Universal LLM API caller that supports multiple providers
 * @param {Array} messages - Array of message objects with role and content
 * @param {number} temperature - Temperature for generation (0-1)
 * @returns {Promise<string>} - Generated text response
 */
async function callLLM(messages, temperature = 0.1) {
  const provider = PROVIDERS[LLM_PROVIDER];

  if (!provider) {
    throw new Error(`Invalid LLM_PROVIDER: ${LLM_PROVIDER}. Must be 'openrouter' or 'google'`);
  }

  // Validate API key
  const apiKey = LLM_PROVIDER === 'openrouter' ? OPENROUTER_API_KEY : GOOGLE_API_KEY;
  if (!apiKey) {
    throw new Error(`${LLM_PROVIDER.toUpperCase()}_API_KEY is not configured`);
  }

  // Determine model to use
  const model = LLM_MODEL || provider.defaultModel;

  // Build request
  const url = typeof provider.url === 'function'
    ? provider.url(model, apiKey)
    : provider.url;

  const headers = provider.getHeaders(apiKey);
  const body = provider.formatRequest(model, messages, temperature);

  logger.info('LLM API call', {
    provider: LLM_PROVIDER,
    model,
    messageCount: messages.length
  });

  // Make API call
  const response = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(body)
  });

  if (!response.ok) {
    const errorText = await response.text();
    let errorDetails = '';

    try {
      const errorJson = JSON.parse(errorText);
      errorDetails = errorJson.error?.message || errorJson.message || errorText;
    } catch {
      errorDetails = errorText;
    }

    // Provider-specific error handling
    if (response.status === 402) {
      logger.error('💳 LLM API: INSUFFICIENT CREDITS', {
        provider: LLM_PROVIDER,
        status: response.status,
        error: errorDetails
      });
    } else if (response.status === 401 || response.status === 403) {
      logger.error('🔑 LLM API: AUTHENTICATION FAILED', {
        provider: LLM_PROVIDER,
        status: response.status,
        error: errorDetails
      });
    } else if (response.status === 429) {
      logger.error('⏱️ LLM API: RATE LIMIT EXCEEDED', {
        provider: LLM_PROVIDER,
        status: response.status,
        error: errorDetails
      });
    } else {
      logger.error('❌ LLM API call failed', {
        provider: LLM_PROVIDER,
        status: response.status,
        error: errorDetails
      });
    }

    throw new Error(`LLM API error (${response.status}): ${errorDetails}`);
  }

  const data = await response.json();
  const content = provider.extractResponse(data);

  if (!content) {
    logger.warn('⚠️ LLM response was empty');
    throw new Error('Empty response from LLM');
  }

  return content;
}

/**
 * Get current LLM provider info
 */
function getProviderInfo() {
  return {
    provider: LLM_PROVIDER,
    model: LLM_MODEL || PROVIDERS[LLM_PROVIDER]?.defaultModel,
    configured: LLM_PROVIDER === 'openrouter' ? !!OPENROUTER_API_KEY : !!GOOGLE_API_KEY
  };
}

module.exports = {
  callLLM,
  getProviderInfo
};
