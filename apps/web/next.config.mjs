/** @type {import('next').NextConfig} */
const nextConfig = {
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
