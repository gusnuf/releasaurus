//! Implements the Forge trait for Gitea
use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::DateTime;
use color_eyre::eyre::ContextCompat;
use regex::Regex;
use reqwest::{
    Client, StatusCode,
    header::{HeaderMap, HeaderValue},
};
use secrecy::{ExposeSecret, SecretString};
use std::cmp;
use url::Url;

use crate::{
    config::{
        Config, DEFAULT_COMMIT_SEARCH_DEPTH, DEFAULT_CONFIG_FILE,
        DEFAULT_TAG_SEARCH_DEPTH,
    },
    forge::{
        config::{
            DEFAULT_LABEL_COLOR, DEFAULT_PAGE_SIZE, LEGACY_PENDING_LABEL,
            PENDING_LABEL, RepoUrl, TokenVar, resolve_token,
        },
        gitea::types::{
            CreateLabel, CreatePull, CreateRelease, GiteaCommitQueryObject,
            GiteaCreatedCommit, GiteaFileChange, GiteaFileChangeOperation,
            GiteaModifyFiles, GiteaPullRequest, GiteaRelease, GiteaTag, Label,
            UpdatePullBody, UpdatePullLabels, UpdatePullState,
        },
        request::{
            Commit, CreateCommitRequest, CreatePrRequest,
            CreateReleaseBranchRequest, FileUpdateType, ForgeCommit,
            GetFileContentRequest, GetPrRequest, PrLabelsRequest, PullRequest,
            ReleaseByTagResponse, Tag, UpdatePrRequest,
        },
        traits::Forge,
    },
    result::{ReleasaurusError, Result},
};

mod types;

/// Gitea forge implementation using reqwest for API interactions with
/// commit history, tags, pull requests, and releases.
pub struct Gitea {
    url: RepoUrl,
    commit_search_depth: usize,
    tag_search_depth: usize,
    base_url: Url,
    client: Client,
    default_branch: String,
    release_link_base_url: Url,
    compare_link_base_url: Url,
}

impl Gitea {
    /// Create Gitea client with token authentication and API base URL
    /// configuration for self-hosted instances.
    pub async fn new(
        url: RepoUrl,
        token: Option<SecretString>,
    ) -> Result<Self> {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .ok();

        let token = resolve_token(token, url.token.as_ref(), TokenVar::Gitea)?;

        let link_base_url = url.link_base_url();

        let release_link_base_url = Url::parse(&format!(
            "{}/{}/{}/releases/",
            link_base_url, url.owner, url.name
        ))?;

        let compare_link_base_url = Url::parse(&format!(
            "{}/{}/{}/compare/",
            link_base_url, url.owner, url.name
        ))?;

        let mut headers = HeaderMap::new();

        let token_value = HeaderValue::from_str(
            format!("token {}", token.expose_secret()).as_str(),
        )?;

        headers.append("Authorization", token_value);

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        let base_url = match url.port {
            Some(port) => format!(
                "{}://{}:{}/api/v1/repos/{}/{}/",
                url.scheme, url.host, port, url.owner, url.name
            ),
            None => format!(
                "{}://{}/api/v1/repos/{}/{}/",
                url.scheme, url.host, url.owner, url.name
            ),
        };

        let base_url = Url::parse(&base_url)?;

        let request = client.get(base_url.clone()).build()?;
        let response = client.execute(request).await?;
        let result = response.error_for_status()?;
        let repo: serde_json::Value = result.json().await?;
        let default_branch = repo["default_branch"]
            .as_str()
            .wrap_err("failed to get default branch")?;

        Ok(Self {
            url,
            commit_search_depth: DEFAULT_COMMIT_SEARCH_DEPTH,
            tag_search_depth: DEFAULT_TAG_SEARCH_DEPTH,
            client,
            base_url,
            release_link_base_url,
            compare_link_base_url,
            default_branch: default_branch.into(),
        })
    }

    async fn get_file_sha(&self, path: &str) -> Result<String> {
        let path = path.strip_prefix("./").unwrap_or(path);
        let file_url = self.base_url.join(&format!("contents/{path}"))?;
        let request = self.client.get(file_url).build()?;
        let response = self.client.execute(request).await?;
        let result = response.error_for_status()?;
        let file: serde_json::Value = result.json().await?;
        let sha = file["sha"].as_str().wrap_err("failed to get file sha")?;
        Ok(sha.into())
    }

    async fn branch_exists(&self, name: &str) -> Result<bool> {
        let url = self.base_url.join(&format!("branches/{name}"))?;
        let response = self.client.get(url).send().await?;
        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => {
                response.error_for_status()?;
                Ok(false)
            }
        }
    }

    async fn delete_branch(&self, name: &str) -> Result<()> {
        let url = self.base_url.join(&format!("branches/{name}"))?;
        let response = self.client.delete(url).send().await?;
        response.error_for_status()?;
        Ok(())
    }

    async fn set_pr_state(&self, pr_number: u64, state: &str) -> Result<()> {
        let url = self.base_url.join(&format!("pulls/{pr_number}"))?;
        let body = UpdatePullState {
            state: state.into(),
        };
        let response = self.client.patch(url).json(&body).send().await?;
        response.error_for_status()?;
        Ok(())
    }

    async fn get_all_labels(&self) -> Result<Vec<Label>> {
        // must paginate: callers rely on "all" being literal, otherwise
        // a present label can look absent past page 1
        let mut labels = vec![];
        let mut has_more = true;
        let mut page = 1;
        let page_limit = DEFAULT_PAGE_SIZE.to_string();

        while has_more {
            let mut labels_url = self.base_url.join("labels")?;
            labels_url
                .query_pairs_mut()
                .append_pair("limit", &page_limit)
                .append_pair("page", &page.to_string());

            let request = self.client.get(labels_url).build()?;
            let response = self.client.execute(request).await?;
            let headers = response.headers();

            has_more = headers
                .get("x-hasmore")
                .map(|h| h.to_str().unwrap() == "true")
                .unwrap_or(false);

            let result = response.error_for_status()?;
            let page_labels: Vec<Label> = result.json().await?;
            labels.extend(page_labels);

            page += 1;
        }

        Ok(labels)
    }

    async fn create_label(&self, label_name: String) -> Result<Label> {
        let labels_url = self.base_url.join("labels")?;
        let request = self
            .client
            .post(labels_url)
            .json(&CreateLabel {
                name: label_name,
                color: DEFAULT_LABEL_COLOR.to_string(),
            })
            .build()?;
        let response = self.client.execute(request).await?;
        let result = response.error_for_status()?;
        let label: Label = result.json().await?;
        Ok(label)
    }

    /// Returns true if `tag_sha` is an ancestor of `branch`. Uses the
    /// Gitea compare API: `total_commits == 0` when base=branch, head=tag
    /// means the tag has no commits that the branch doesn't, so the tag
    /// is fully contained within the branch's history.
    async fn is_tag_ancestor_of_branch(
        &self,
        tag_sha: &str,
        branch: &str,
    ) -> Result<bool> {
        let compare_url = Url::parse(&format!(
            "{}compare/{}...{}",
            self.base_url.as_str(),
            branch,
            tag_sha,
        ))?;
        let request = self.client.get(compare_url).build()?;
        let response = self.client.execute(request).await?;
        let result: serde_json::Value =
            response.error_for_status()?.json().await?;
        Ok(result["total_commits"].as_u64() == Some(0))
    }
}

#[async_trait]
impl Forge for Gitea {
    fn repo_name(&self) -> String {
        self.url.name.clone()
    }

    fn release_link_base_url(&self) -> Url {
        self.release_link_base_url.clone()
    }

    fn compare_link_base_url(&self) -> Url {
        self.compare_link_base_url.clone()
    }

    fn default_branch(&self) -> String {
        self.default_branch.clone()
    }

    fn set_commit_search_depth(&mut self, depth: usize) {
        self.commit_search_depth = if depth == 0 { usize::MAX } else { depth }
    }

    fn set_tag_search_depth(&mut self, depth: usize) {
        self.tag_search_depth = if depth == 0 { usize::MAX } else { depth }
    }

    async fn get_file_content(
        &self,
        req: GetFileContentRequest,
    ) -> Result<Option<String>> {
        let mut raw_url = self.base_url.join(&format!("raw/{}", req.path))?;
        if let Some(branch) = req.branch {
            raw_url = self
                .base_url
                .join(&format!("raw/{}?ref={branch}", req.path))?;
        }
        let request = self.client.get(raw_url).build()?;
        let response = self.client.execute(request).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let result = response.error_for_status()?;
        let content = result.text().await?;
        Ok(Some(content))
    }

    async fn load_config(&self, branch: Option<String>) -> Result<Config> {
        if let Some(content) = self
            .get_file_content(GetFileContentRequest {
                branch,
                path: DEFAULT_CONFIG_FILE.into(),
            })
            .await?
        {
            let config: Config = toml::from_str(&content)?;

            Ok(config)
        } else {
            Ok(Config::default())
        }
    }

    async fn get_release_by_tag(
        &self,
        tag: &str,
    ) -> Result<ReleaseByTagResponse> {
        let tag_endpoint = self.base_url.join(&format!("tags/{tag}"))?;
        let request = self.client.get(tag_endpoint).build()?;
        let response = self.client.execute(request).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(ReleasaurusError::forge(format!(
                "tag not found: {tag}"
            )));
        }
        let result = response.error_for_status()?;
        let tag: GiteaTag = result.json().await?;

        let release_endpoint =
            self.base_url.join(&format!("releases/tags/{}", tag.name))?;
        let request = self.client.get(release_endpoint).build()?;
        let response = self.client.execute(request).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(ReleasaurusError::forge(format!(
                "no release found for tag: {}",
                tag.name
            )));
        }
        let result = response.error_for_status()?;
        let release: GiteaRelease = result.json().await?;

        Ok(ReleaseByTagResponse {
            tag: tag.name.clone(),
            sha: tag.commit.sha.clone(),
            notes: release.body.clone(),
        })
    }

    // We return only tags that matches the prefix AND are ancestors of
    // the target base branch.
    async fn get_latest_tags_for_prefix(
        &self,
        prefix: &str,
        branch: &str,
    ) -> Result<Vec<Tag>> {
        let re = Regex::new(format!(r"^{prefix}").as_str())?;
        let mut has_more = true;
        let mut page = 1;
        let page_limit = DEFAULT_PAGE_SIZE.to_string();
        let mut count = 0;

        let mut tags = vec![];

        while has_more {
            let mut tags_url = self.base_url.join("tags")?;

            tags_url
                .query_pairs_mut()
                .append_pair("limit", &page_limit)
                .append_pair("page", &page.to_string());

            let request = self.client.get(tags_url).build()?;
            let response = self.client.execute(request).await?;

            let headers = response.headers();

            has_more = headers
                .get("x-hasmore")
                .map(|h| h.to_str().unwrap() == "true")
                .unwrap_or(false);

            let result = response.error_for_status()?;
            let page_tags: Vec<GiteaTag> = result.json().await?;

            for tag in page_tags.into_iter() {
                if count >= self.tag_search_depth {
                    has_more = false;
                    break;
                }
                count += 1;
                if re.is_match(&tag.name) {
                    let stripped = re.replace_all(&tag.name, "").to_string();
                    if let Ok(sver) = semver::Version::parse(&stripped)
                        && self
                            .is_tag_ancestor_of_branch(&tag.commit.sha, branch)
                            .await?
                    {
                        tags.push(Tag {
                            name: tag.name,
                            semver: sver,
                            sha: tag.commit.sha,
                            timestamp: DateTime::parse_from_rfc3339(
                                &tag.commit.created,
                            )
                            .map(|t| t.timestamp())
                            .ok(),
                        });
                    }
                }
            }

            page += 1;
        }

        Ok(tags)
    }

    async fn get_commits(
        &self,
        branch: Option<String>,
        sha: Option<String>,
    ) -> Result<Vec<ForgeCommit>> {
        let mut page = 1;
        let page_limit =
            cmp::min(DEFAULT_PAGE_SIZE.into(), self.commit_search_depth);
        let mut has_more = true;
        let mut count = 0;
        let mut commits: Vec<ForgeCommit> = vec![];

        let mut since = None;

        if let Some(sha) = sha.clone() {
            let commit_url =
                self.base_url.join(&format!("git/commits/{sha}"))?;
            let request = self.client.get(commit_url).build()?;
            let response = self.client.execute(request).await?;
            let result = response.error_for_status()?;
            let commit: GiteaCommitQueryObject = result.json().await?;
            since = Some(commit.created);
        }

        while has_more {
            let mut commits_url = self.base_url.join("commits")?;

            commits_url
                .query_pairs_mut()
                .append_pair("limit", &page_limit.to_string())
                .append_pair("page", &page.to_string());

            if let Some(branch) = branch.clone() {
                commits_url.query_pairs_mut().append_pair("sha", &branch);
            }

            if let Some(since) = since.clone() {
                commits_url.query_pairs_mut().append_pair("since", &since);
            }

            let request = self.client.get(commits_url).build()?;
            let response = self.client.execute(request).await?;
            let headers = response.headers();

            has_more = headers
                .get("x-hasmore")
                .map(|h| h.to_str().unwrap() == "true")
                .unwrap_or(false);

            let result = response.error_for_status()?;
            let results: Vec<GiteaCommitQueryObject> = result.json().await?;

            for result in results.iter() {
                // only apply search depth if this is the first release
                if sha.is_none() && count >= self.commit_search_depth {
                    return Ok(commits);
                }

                // we've reached the target sha stopping point
                // this is because "since" is inclusive of the target commit
                if let Some(sha) = sha.clone()
                    && sha == result.sha
                {
                    return Ok(commits);
                }

                let timestamp =
                    DateTime::parse_from_rfc3339(&result.created)?.timestamp();

                let forge_commit = ForgeCommit {
                    author_email: result.commit.author.email.clone(),
                    author_name: result.commit.author.name.clone(),
                    id: result.sha.clone(),
                    short_id: result.sha.chars().take(8).collect::<String>(),
                    link: result.html_url.clone(),
                    merge_commit: result.parents.len() > 1,
                    message: result.commit.message.trim().to_string(),
                    timestamp,
                    files: result
                        .files
                        .iter()
                        .map(|f| f.filename.clone())
                        .collect::<Vec<String>>(),
                };

                commits.push(forge_commit);
                count += 1;
            }

            page += 1;
        }

        Ok(commits)
    }

    async fn create_release_branch(
        &self,
        req: CreateReleaseBranchRequest,
    ) -> Result<Commit> {
        // forgejo's POST /contents with new_branch set 422s with
        // "branch already exists" when the target branch already exists,
        // even with force_push=true — there's no API to force-update an
        // existing branch's HEAD. for cycles after the first we delete
        // the existing release branch so the new commit lands fresh on
        // top of base_branch. delete_branch auto-closes any open PR
        // pointing at the deleted head; capture it first and reopen
        // afterwards so reviews, labels, and PR number survive.
        let pr_to_reopen = if self.branch_exists(&req.release_branch).await? {
            let existing_pr = self
                .get_open_release_pr(GetPrRequest {
                    base_branch: req.base_branch.clone(),
                    head_branch: req.release_branch.clone(),
                })
                .await?;
            self.delete_branch(&req.release_branch).await?;
            existing_pr.map(|pr| pr.number)
        } else {
            None
        };

        let mut file_changes: Vec<GiteaFileChange> = vec![];

        for change in req.file_changes.iter() {
            let mut op = GiteaFileChangeOperation::Update;
            let mut sha = None;
            let mut content = change.content.clone();
            let existing_content = self
                .get_file_content(GetFileContentRequest {
                    branch: Some(req.base_branch.clone()),
                    path: change.path.to_string(),
                })
                .await?;
            if let Some(existing_content) = existing_content {
                sha = Some(self.get_file_sha(&change.path).await?);
                if matches!(change.update_type, FileUpdateType::Prepend) {
                    content = format!("{content}{existing_content}");
                }
            } else {
                op = GiteaFileChangeOperation::Create;
            }
            file_changes.push(GiteaFileChange {
                path: change.path.clone(),
                content: BASE64_STANDARD.encode(&content),
                operation: op,
                sha,
            })
        }

        let body = GiteaModifyFiles {
            branch: req.base_branch,
            new_branch: Some(req.release_branch),
            message: req.message,
            files: file_changes,
            force_push: true,
        };

        let contents_url = self.base_url.join("contents")?;
        let request = self.client.post(contents_url).json(&body).build()?;
        let response = self.client.execute(request).await?;
        let result = response.error_for_status()?;
        let created: GiteaCreatedCommit = result.json().await?;

        if let Some(pr_number) = pr_to_reopen {
            self.set_pr_state(pr_number, "open").await?;
        }

        Ok(created.commit)
    }

    async fn create_commit(&self, req: CreateCommitRequest) -> Result<Commit> {
        let mut file_changes: Vec<GiteaFileChange> = vec![];

        for change in req.file_changes.iter() {
            let mut op = GiteaFileChangeOperation::Update;
            let mut sha = None;
            let mut content = change.content.clone();
            let existing_content = self
                .get_file_content(GetFileContentRequest {
                    branch: Some(req.target_branch.clone()),
                    path: change.path.to_string(),
                })
                .await?;
            if let Some(existing_content) = existing_content.clone() {
                sha = Some(self.get_file_sha(&change.path).await?);
                if matches!(change.update_type, FileUpdateType::Prepend) {
                    content = format!("{content}{existing_content}");
                }
            } else {
                op = GiteaFileChangeOperation::Create;
            }

            if content == existing_content.unwrap_or_default() {
                log::warn!(
                    "skipping file update content matches existing state: {}",
                    change.path
                );
                continue;
            }

            file_changes.push(GiteaFileChange {
                path: change.path.clone(),
                content: BASE64_STANDARD.encode(&content),
                operation: op,
                sha,
            })
        }

        if file_changes.is_empty() {
            log::warn!(
                "commit would result in no changes: target_branch: {}, message: {}",
                req.target_branch,
                req.message,
            );
            return Ok(Commit { sha: "None".into() });
        }

        let body = GiteaModifyFiles {
            new_branch: None,
            branch: req.target_branch,
            message: req.message,
            files: file_changes,
            force_push: false,
        };

        let contents_url = self.base_url.join("contents")?;
        let request = self.client.post(contents_url).json(&body).build()?;
        let response = self.client.execute(request).await?;
        let result = response.error_for_status()?;
        let created: GiteaCreatedCommit = result.json().await?;

        Ok(created.commit)
    }

    async fn tag_commit(&self, tag_name: &str, sha: &str) -> Result<()> {
        let tag_url = self.base_url.join("tags")?;
        let body = serde_json::json!({
          "tag_name": tag_name,
          "target": sha
        });
        let request = self.client.post(tag_url).json(&body).build()?;
        let response = self.client.execute(request).await?;

        // idempotent on same-sha 409: tag already points where we want it
        if response.status() == StatusCode::CONFLICT {
            let existing_url =
                self.base_url.join(&format!("tags/{tag_name}"))?;
            let existing_req = self.client.get(existing_url).build()?;
            let existing_resp = self.client.execute(existing_req).await?;
            let existing: GiteaTag =
                existing_resp.error_for_status()?.json().await?;
            if existing.commit.sha == sha {
                log::info!(
                    "tag {tag_name} already exists at {sha}, treating as success"
                );
                return Ok(());
            }
            return Err(ReleasaurusError::forge(format!(
                "tag {tag_name} already exists pointing at {} but expected {sha}",
                existing.commit.sha
            )));
        }

        response.error_for_status()?;
        Ok(())
    }

    async fn get_open_release_pr(
        &self,
        req: GetPrRequest,
    ) -> Result<Option<PullRequest>> {
        // forgejo/gitea `labels=` requires numeric ids — passing a name
        // silently returns the unfiltered set
        let all_labels = self.get_all_labels().await?;
        let label_ids: Vec<u64> = [PENDING_LABEL, LEGACY_PENDING_LABEL]
            .iter()
            .filter_map(|name| {
                all_labels.iter().find(|l| l.name == *name).map(|l| l.id)
            })
            .collect();

        if label_ids.is_empty() {
            return Ok(None);
        }

        let mut found_prs = vec![];

        // Try the current label first, then fall back to the
        // legacy single-colon label for users upgrading from an
        // older version of releasaurus.
        for label_id in label_ids {
            if !found_prs.is_empty() {
                break;
            }

            let mut has_more = true;
            let mut page = 1;
            let page_limit = DEFAULT_PAGE_SIZE.to_string();
            let label_id_str = label_id.to_string();

            while has_more {
                // forgejo's /issues endpoint silently drops the
                // `labels=` filter; /pulls honors it. see
                // https://codeberg.org/forgejo/forgejo for context.
                let mut pulls_url = self.base_url.join("pulls")?;

                pulls_url
                    .query_pairs_mut()
                    .append_pair("state", "open")
                    .append_pair("labels", &label_id_str)
                    .append_pair("limit", &page_limit.to_string())
                    .append_pair("page", &page.to_string());

                let request = self.client.get(pulls_url).build()?;
                let response = self.client.execute(request).await?;
                let headers = response.headers();

                has_more = headers
                    .get("x-hasmore")
                    .map(|h| h.to_str().unwrap() == "true")
                    .unwrap_or(false);

                let result = response.error_for_status()?;
                let prs: Vec<GiteaPullRequest> = result.json().await?;

                for found_pr in prs {
                    if found_pr.head.label == req.head_branch {
                        found_prs.push(PullRequest {
                            number: found_pr.number,
                            sha: found_pr.head.sha,
                            body: found_pr.body,
                        });
                    }
                }

                page += 1;
            }
        }

        if found_prs.is_empty() {
            return Ok(None);
        }

        if found_prs.len() > 1 {
            return Err(ReleasaurusError::forge(format!(
                "Found more than one open release PR with pending label for branch {}",
                req.head_branch
            )));
        }

        Ok(Some(PullRequest {
            number: found_prs[0].number,
            sha: found_prs[0].sha.clone(),
            body: found_prs[0].body.clone(),
        }))
    }

    async fn get_merged_release_pr(
        &self,
        req: GetPrRequest,
    ) -> Result<Option<PullRequest>> {
        // forgejo/gitea `labels=` requires numeric ids — passing a name
        // silently returns the unfiltered set
        let all_labels = self.get_all_labels().await?;
        let label_ids: Vec<u64> = [PENDING_LABEL, LEGACY_PENDING_LABEL]
            .iter()
            .filter_map(|name| {
                all_labels.iter().find(|l| l.name == *name).map(|l| l.id)
            })
            .collect();

        if label_ids.is_empty() {
            return Ok(None);
        }

        let mut found_prs = vec![];

        // Try the current label first, then fall back to the
        // legacy single-colon label for users upgrading from an
        // older version of releasaurus.
        for label_id in label_ids {
            if !found_prs.is_empty() {
                break;
            }

            let mut has_more = true;
            let mut page = 1;
            let page_limit = DEFAULT_PAGE_SIZE.to_string();
            let label_id_str = label_id.to_string();

            while has_more {
                // forgejo's /issues endpoint silently drops the
                // `labels=` filter; /pulls honors it.
                let mut pulls_url = self.base_url.join("pulls")?;

                pulls_url
                    .query_pairs_mut()
                    .append_pair("state", "closed")
                    .append_pair("labels", &label_id_str)
                    .append_pair("limit", &page_limit.to_string())
                    .append_pair("page", &page.to_string());

                let request = self.client.get(pulls_url).build()?;
                let response = self.client.execute(request).await?;
                let headers = response.headers();

                has_more = headers
                    .get("x-hasmore")
                    .map(|h| h.to_str().unwrap() == "true")
                    .unwrap_or(false);

                let result = response.error_for_status()?;
                let prs: Vec<GiteaPullRequest> = result.json().await?;

                for found_pr in prs {
                    if !found_pr.merged {
                        log::warn!(
                            "found unmerged closed pr {} with pending label: skipping",
                            found_pr.number
                        );
                        continue;
                    }

                    if found_pr.head.label == req.head_branch {
                        let sha =
                            found_pr.merge_commit_sha.ok_or_else(|| {
                                ReleasaurusError::forge(format!(
                                    "no merge_commit_sha found for pr {}",
                                    found_pr.number
                                ))
                            })?;
                        found_prs.push(PullRequest {
                            number: found_pr.number,
                            sha,
                            body: found_pr.body,
                        });
                    }
                }

                page += 1;
            }
        }

        if found_prs.is_empty() {
            return Ok(None);
        }

        if found_prs.len() > 1 {
            return Err(ReleasaurusError::forge(format!(
                "Found more than one closed release PR with pending label for branch {}. \
              This means either release PRs were closed manually or releasaurus failed to remove tags. \
              You must remove the {PENDING_LABEL} label from all closed release PRs except for the most recent.",
                req.head_branch
            )));
        }

        Ok(Some(PullRequest {
            number: found_prs[0].number,
            sha: found_prs[0].sha.clone(),
            body: found_prs[0].body.clone(),
        }))
    }

    async fn create_pr(&self, req: CreatePrRequest) -> Result<PullRequest> {
        let data = CreatePull {
            title: req.title,
            body: req.body,
            head: req.head_branch,
            base: req.base_branch,
        };
        let pulls_url = self.base_url.join("pulls")?;
        let request = self.client.post(pulls_url).json(&data).build()?;
        let response = self.client.execute(request).await?;
        let result = response.error_for_status()?;
        let pr: GiteaPullRequest = result.json().await?;

        Ok(PullRequest {
            number: pr.number,
            sha: pr.head.sha,
            body: pr.body,
        })
    }

    async fn update_pr(&self, req: UpdatePrRequest) -> Result<()> {
        let data = UpdatePullBody {
            title: req.title,
            body: req.body,
        };
        let pulls_url = self
            .base_url
            .join(format!("pulls/{}", req.pr_number).as_str())?;
        let request = self.client.patch(pulls_url).json(&data).build()?;
        let response = self.client.execute(request).await?;
        response.error_for_status()?;
        Ok(())
    }

    async fn replace_pr_labels(&self, req: PrLabelsRequest) -> Result<()> {
        let all_labels = self.get_all_labels().await?;

        let mut labels = vec![];

        for name in req.labels {
            if let Some(label) = all_labels.iter().find(|l| l.name == name) {
                labels.push(label.id);
            } else {
                let label = self.create_label(name).await?;
                labels.push(label.id);
            }
        }

        let data = UpdatePullLabels { labels };

        let labels_url = self
            .base_url
            .join(format!("issues/{}/labels", req.pr_number).as_str())?;

        let request = self.client.put(labels_url).json(&data).build()?;
        let response = self.client.execute(request).await?;
        response.error_for_status()?;

        Ok(())
    }

    async fn create_release(
        &self,
        tag: &str,
        sha: &str,
        notes: &str,
    ) -> Result<()> {
        let data = CreateRelease {
            tag_name: tag.to_string(),
            target_commitish: sha.to_string(),
            name: tag.to_string(),
            body: notes.to_string(),
            draft: false,
            prerelease: false,
        };

        let releases_url = self.base_url.join("releases")?;
        let request = self.client.post(releases_url).json(&data).build()?;
        let response = self.client.execute(request).await?;
        response.error_for_status()?;

        Ok(())
    }
}
