/** @type {import('next').NextConfig} */
const nextConfig = {
    webpack: (config, { isServer }) => {
        config.module.rules.push({
            test: /\.svg$/,
            use: ['@svgr/webpack'],
        },
        {  
            test: /\.ts?$/,
            use: [
                {
                    loader: 'ts-loader',
                    options: {
                        transpileOnly: true,
                    },
                },
            ],
        });
        config.resolve.extensions.push('.ts', '.tsx');
        return config;
    },
}

module.exports = nextConfig
