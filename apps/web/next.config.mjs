const isCleanStartDev = process.env.FRATFINDER_WEB_DEV_CLEAN_START === "true";
const explicitDistDir = process.env.FRATFINDER_WEB_DIST_DIR?.trim();

/** @type {import('next').NextConfig} */
const nextConfig = {
  distDir: explicitDistDir || (isCleanStartDev ? ".next-dev" : ".next"),
  webpack: (config, { dev }) => {
    if (dev) {
      // Disable filesystem cache in dev to avoid stale route bundle lookups
      // on local Windows reload cycles.
      config.cache = false;
    }
    return config;
  }
};

export default nextConfig;
