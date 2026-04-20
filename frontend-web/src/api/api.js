import axios from "axios";
import { supabase } from "../shared/supabase";

const API = axios.create({
    baseURL: import.meta.env.VITE_PARSER_API_URL || "http://localhost:8000",
});

// Attach the live Supabase JWT automatically on every request
API.interceptors.request.use(async (config) => {
    if (supabase) {
        const { data } = await supabase.auth.getSession();
        const token = data?.session?.access_token;
        if (token) {
            config.headers = config.headers ?? {};
            config.headers["Authorization"] = `Bearer ${token}`;
        }
    }
    return config;
});

// Handle 401 errors
API.interceptors.response.use(
    (response) => response,
    (error) => {
        if (error.response && error.response.status === 401) {
            console.error("401 Unauthorized - parser API request rejected. Supabase session may be missing or expired.");
        }
        return Promise.reject(error);
    }
);

export default API;
