# Seaport-NFT-analysis

This project uses two main CSV files for NFT trading data analysis:

## 1. `alchemy_nft_sales_recent.csv`
- **Source**: Exported using Alchemy API (Seaport data stream).
- **Purpose**: Used for forecasting and anomaly detection shown in Figures 1–2.
- **Note**: Timestamp field was reconstructed from `block_number` due to missing date values.

## 2. `GraphQL_all_sales_stream.csv`
- **Source**: Retrieved via GraphQL queries from The Graph Protocol.
- **Purpose**: Used in most of our time-series and anomaly analysis (e.g., Figures 3–6).
- **Includes**: Accurate timestamps, buyer/seller addresses, token info, and sale prices.

Both datasets were preprocessed using Python (Pandas), and used in various trend, anomaly, and wallet-level analyses as documented in the report.
