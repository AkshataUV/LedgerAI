# Categorization Engine Upgrade: Simple Explanation

Based on our conversation, the proposed solution to make the categorization engine much stronger and reduce LLM costs is to implement a **Two-Pillar Upgrade**:

## Pillar 1: Implementation of "Category Anchors"
Instead of only storing specific merchant names (like "Zomato"), we will add **Conceptual Anchors** to the Global Vector Database.

*   **The Change**: We add "ideal" vectors for words like `CATERING`, `EATERY`, `SNACKS`, `TEA STALL`, and `GROCERY` to our system.
*   **The Result**: When a word like "POHA" or "CHAI" comes in, the Vector engine will see it is 90% similar to the `SNACKS` anchor and immediately categorize it as **Food**, bypassing the expensive and slow LLM.

## Pillar 2: "Tie-Breaker" Logic for Ambiguity
We will add a mid-tier logic layer specifically for words like "HOTEL" that can belong to two categories (Food or Travel).

### Amount-Based Filtering:
*   **HOTEL + Amount < ₹1,500** = Automatically map to **Food & Dining**.
*   **HOTEL + Amount > ₹4,000** = Automatically map to **Travel/Stay**.

### Contextual Suffixes: 
Rules that prioritize **"HOTEL RESTAURANT"** as Food and **"HOTEL LODGING"** as Travel before they ever hit the Vector stage.

---

## Why this is better:
1.  **Lower Cost**: You save significantly on LLM API tokens.
2.  **Higher Speed**: Vector math takes ~5ms, while an LLM response takes ~2-3 seconds.
3.  **Better Accuracy**: It uses **"Indian context"** (using the amount as a clue) which global AI models (like Gemini or OpenAI) sometimes miss.

---

## Technical Appendix (For Developers)

### 1. The Challenge (Why it fails today)
*   **Short words**: Words like "TEA" are too short for the 0.80 similarity threshold.
*   **Ambiguity**: "HOTEL" has two meanings. Vectors alone can't decide between a meal and a room stay.

### 2. Implementation Roadmap
1.  **Phase 1: Seed the Database**: Add anchor words (Tea, Food, Medicine) to the `global_vector_cache` table.
2.  **Phase 2: Code Update**: Add an `applyTieBreakers` function in `vectorMatchService.js` that checks the amount before doing the vector search.
3.  **Phase 3: Verify**: Monitor the logs to see how many transactions are now "Auto-Categorized" instead of going to the LLM.
