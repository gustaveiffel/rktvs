// SPDX-License-Identifier: AGPL-3.0-only

/// Transfer job priority, inspired by UUCP grades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum Grade {
    Urgent = 0,
    Normal = 1,
    Batch = 2,
    Background = 3,
}

impl Grade {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            0 => Some(Self::Urgent),
            1 => Some(Self::Normal),
            2 => Some(Self::Batch),
            3 => Some(Self::Background),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Urgent => "urgent",
            Self::Normal => "normal",
            Self::Batch => "batch",
            Self::Background => "background",
        }
    }
}

impl std::str::FromStr for Grade {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "urgent" => Ok(Self::Urgent),
            "normal" => Ok(Self::Normal),
            "batch" => Ok(Self::Batch),
            "background" => Ok(Self::Background),
            other => Err(format!("unknown grade: {other}")),
        }
    }
}

/// Classification of the active network link.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum CostTier {
    Free = 0,
    Cheap = 1,
    Metered = 2,
    Expensive = 3,
}

impl CostTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Free => "free",
            Self::Cheap => "cheap",
            Self::Metered => "metered",
            Self::Expensive => "expensive",
        }
    }
}

impl std::str::FromStr for CostTier {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "free" => Ok(Self::Free),
            "cheap" => Ok(Self::Cheap),
            "metered" => Ok(Self::Metered),
            "expensive" => Ok(Self::Expensive),
            other => Err(format!("unknown cost tier: {other}")),
        }
    }
}

/// Decision from the Grade x CostTier matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Run,
    Queue,
}

/// Evaluate whether a job should run given the current cost tier.
pub fn decide(grade: Grade, cost: CostTier) -> Decision {
    match grade {
        Grade::Urgent => Decision::Run,
        Grade::Normal => {
            if cost <= CostTier::Metered {
                Decision::Run
            } else {
                Decision::Queue
            }
        }
        Grade::Batch => {
            if cost <= CostTier::Cheap {
                Decision::Run
            } else {
                Decision::Queue
            }
        }
        Grade::Background => {
            if cost == CostTier::Free {
                Decision::Run
            } else {
                Decision::Queue
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urgent_always_runs() {
        assert_eq!(decide(Grade::Urgent, CostTier::Free), Decision::Run);
        assert_eq!(decide(Grade::Urgent, CostTier::Expensive), Decision::Run);
    }

    #[test]
    fn normal_runs_up_to_metered() {
        assert_eq!(decide(Grade::Normal, CostTier::Free), Decision::Run);
        assert_eq!(decide(Grade::Normal, CostTier::Cheap), Decision::Run);
        assert_eq!(decide(Grade::Normal, CostTier::Metered), Decision::Run);
        assert_eq!(decide(Grade::Normal, CostTier::Expensive), Decision::Queue);
    }

    #[test]
    fn batch_runs_up_to_cheap() {
        assert_eq!(decide(Grade::Batch, CostTier::Free), Decision::Run);
        assert_eq!(decide(Grade::Batch, CostTier::Cheap), Decision::Run);
        assert_eq!(decide(Grade::Batch, CostTier::Metered), Decision::Queue);
        assert_eq!(decide(Grade::Batch, CostTier::Expensive), Decision::Queue);
    }

    #[test]
    fn background_runs_only_if_free() {
        assert_eq!(decide(Grade::Background, CostTier::Free), Decision::Run);
        assert_eq!(decide(Grade::Background, CostTier::Cheap), Decision::Queue);
        assert_eq!(
            decide(Grade::Background, CostTier::Metered),
            Decision::Queue
        );
        assert_eq!(
            decide(Grade::Background, CostTier::Expensive),
            Decision::Queue
        );
    }

    #[test]
    fn grade_from_str() {
        assert_eq!("urgent".parse::<Grade>().unwrap(), Grade::Urgent);
        assert_eq!("Normal".parse::<Grade>().unwrap(), Grade::Normal);
        assert_eq!("BATCH".parse::<Grade>().unwrap(), Grade::Batch);
        assert_eq!("background".parse::<Grade>().unwrap(), Grade::Background);
        assert!("invalid".parse::<Grade>().is_err());
    }

    #[test]
    fn cost_tier_from_str() {
        assert_eq!("free".parse::<CostTier>().unwrap(), CostTier::Free);
        assert_eq!(
            "Expensive".parse::<CostTier>().unwrap(),
            CostTier::Expensive
        );
        assert!("invalid".parse::<CostTier>().is_err());
    }
}
