//! Bounded live-HLS audio normalization. Enabled classification is wired separately.

pub mod process;

pub use process::{
    build_command, run_child, BudgetedInput, ChildAdmission, InputBudget, PreparedNormalizer,
};
