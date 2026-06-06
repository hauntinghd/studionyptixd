// Pure re-export to avoid static analysis / circular issues for agent lib files.
// The implementations live in ../shared.tsx (with their required base consts).
export { resolveStudioBackendUrl, studioAgentOAuthReturnUrl } from '../shared';
