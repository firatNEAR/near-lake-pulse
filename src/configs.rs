use clap::Parser;

/// NEAR Lake Pulse
/// Provides metrics data to keep an eye for NEAR Lake data
#[derive(Parser, Debug)]
#[clap(
    version,
    author,
    about,
    disable_help_subcommand(true),
    propagate_version(true),
    next_line_help(true)
)]
pub(crate) struct Opts {
    #[clap(long, short, default_value = "3030")]
    pub http_port: u16,
    #[clap(long, default_value = "10")]
    pub stats_interval_sec: u64,
    #[clap(subcommand)]
    pub chain_id: ChainId,
    #[clap(long)]
    pub accounts: String,
}

#[derive(Parser, Debug)]
pub(crate) enum ChainId {
    Mainnet(RunArgs),
    Testnet(RunArgs),
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct RunArgs {
    /// Block heigh to start watching from
    #[clap(short, long)]
    pub block_height: u64,
}

impl From<ChainId> for near_lake_framework::LakeConfig {
    fn from(chain: ChainId) -> near_lake_framework::LakeConfig {
        let config_builder = near_lake_framework::LakeConfigBuilder::default();

        match chain {
            ChainId::Mainnet(args) => config_builder
                .mainnet()
                .start_block_height(args.block_height)
                .build(),
            ChainId::Testnet(args) => config_builder
                .testnet()
                .start_block_height(args.block_height)
                .build(),
        }
        .expect("Failed to build LakeConfig")
    }
}
