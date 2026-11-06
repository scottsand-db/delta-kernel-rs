//! Reference selection from a complete, atomically observed catalog commit list.

use std::borrow::Cow;

use url::Url;

use super::{LogSegment, validate_catalog_managed_log_tail};
use crate::path::{LogPathFileType, ParsedLogPath, STAGED_COMMITS_DIR};
use crate::utils::require;
use crate::{KernelError, KernelResult, Version};

/// One catalog observation. Callers must validate it before selecting retained references.
#[derive(Debug)]
pub(crate) struct CatalogMetadata {
    pub max_catalog_version: Version,
    pub commits: Vec<ParsedLogPath>,
}

impl CatalogMetadata {
    /// Validate the entire response, including entries beyond a time-travel target.
    pub(crate) fn validate(&self, log_root: &Url, target: Option<Version>) -> KernelResult<()> {
        validate_catalog_managed_log_tail(None, Some(self.max_catalog_version), &self.commits)?;
        if let Some(target) = target {
            require!(
                target <= self.max_catalog_version,
                KernelError::MaxCatalogVersion(format!(
                    "Requested version {target} exceeds max catalog version {}",
                    self.max_catalog_version
                ))
            );
        }
        for commit in &self.commits {
            let parsed = ParsedLogPath::parse_commit(commit.location.clone())?;
            let directory = if parsed.file_type == LogPathFileType::StagedCommit {
                log_root.join(STAGED_COMMITS_DIR)?
            } else {
                log_root.clone()
            };
            require!(
                &parsed == commit && directory.join(&parsed.filename)? == parsed.location.location,
                KernelError::invalid_log_path(format!(
                    "Catalog commit is inconsistent or belongs to another table: {}",
                    commit.location.location
                ))
            );
        }
        Ok(())
    }

    /// Return the first cached version with a conflicting staged identity. Check the original
    /// segment before checkpoint trimming can hide a dependency of the seed's computed state.
    pub(crate) fn first_conflicting_version(&self, segment: &LogSegment) -> Option<Version> {
        segment
            .listed
            .ascending_commit_files
            .iter()
            .chain(&segment.listed.latest_commit_file)
            .filter(|commit| commit.file_type == LogPathFileType::StagedCommit)
            .find_map(|commit| {
                self.commit_at(commit.version).and_then(|catalog| {
                    (catalog.file_type == LogPathFileType::StagedCommit
                        && catalog.location.location != commit.location.location)
                        .then_some(commit.version)
                })
            })
    }

    /// Reconcile replay and latest-commit references without storage requests. A borrowed segment
    /// is cloned only when a reference or publication bound changes. Listed published metadata
    /// takes precedence over inherited staged metadata, but never over a catalog entry.
    pub(crate) fn reconcile<'a>(
        &self,
        mut segment: Cow<'a, LogSegment>,
        observed: &[ParsedLogPath],
    ) -> KernelResult<Cow<'a, LogSegment>> {
        for index in 0..segment.listed.ascending_commit_files.len() {
            let cached = &segment.listed.ascending_commit_files[index];
            let selected = self.select_reference(cached, &segment.log_root, observed)?;
            if selected.as_ref() != cached {
                let selected = selected.into_owned();
                segment.to_mut().listed.ascending_commit_files[index] = selected;
            }
        }
        if let Some(cached) = &segment.listed.latest_commit_file {
            let selected = self.select_reference(cached, &segment.log_root, observed)?;
            if selected.as_ref() != cached {
                let selected = selected.into_owned();
                segment.to_mut().listed.latest_commit_file = Some(selected);
            }
        }
        let published_prefix = self
            .commits
            .first()
            .map_or(Some(self.max_catalog_version), |c| c.version.checked_sub(1));
        let published_entry = self
            .commits
            .iter()
            .filter(|c| c.file_type == LogPathFileType::Commit)
            .map(|c| c.version)
            .max();
        let max_published = segment
            .listed
            .max_published_version
            .max(published_prefix)
            .max(published_entry)
            .map(|version| version.min(segment.end_version));
        if segment.listed.max_published_version != max_published {
            segment.to_mut().listed.max_published_version = max_published;
        }
        Ok(segment)
    }

    fn commit_at(&self, version: Version) -> Option<&ParsedLogPath> {
        self.commits
            .binary_search_by_key(&version, |commit| commit.version)
            .ok()
            .map(|index| &self.commits[index])
    }

    fn select_reference<'a>(
        &'a self,
        cached: &'a ParsedLogPath,
        log_root: &Url,
        observed: &'a [ParsedLogPath],
    ) -> KernelResult<Cow<'a, ParsedLogPath>> {
        if let Some(catalog) = self.commit_at(cached.version) {
            return Ok(Cow::Borrowed(catalog));
        }
        require!(
            cached.version <= self.max_catalog_version,
            KernelError::MaxCatalogVersion(format!(
                "Cached commit version {} exceeds max catalog version {}",
                cached.version, self.max_catalog_version
            ))
        );
        if let Ok(index) = observed.binary_search_by_key(&cached.version, |commit| commit.version)
            && observed[index].file_type == LogPathFileType::Commit
        {
            return Ok(Cow::Borrowed(&observed[index]));
        }
        if cached.file_type == LogPathFileType::StagedCommit {
            cached.to_published(log_root).map(Cow::Owned)
        } else {
            Ok(Cow::Borrowed(cached))
        }
    }
}
