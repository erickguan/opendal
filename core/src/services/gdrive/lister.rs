// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use flagset::FlagSet;
use http::StatusCode;

use super::core::GdriveCore;
use super::core::GdriveFile;
use super::core::GdriveFileList;
use super::error::parse_error;
use crate::raw::*;
use crate::*;
use bytes::Buf;
use chrono::Utc;

pub struct GdriveLister {
    path: String,
    core: Arc<GdriveCore>,
    op: OpList,
}

struct GdriveStatHandle {
    core: Arc<GdriveCore>,
    metakey: FlagSet<Metakey>,
    file_type: EntryMode,
    path: String,
}

async fn get_meta(
    core: Arc<GdriveCore>,
    file_type: EntryMode,
    metakey: FlagSet<Metakey>,
    path: &String,
) -> Result<Metadata, Error> {
    let mut meta = Metadata::new(file_type);
    if metakey.contains(Metakey::ContentLength)
        || metakey.contains(Metakey::LastModified)
    {
        let resp = core.gdrive_stat(&path).await?;

        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();
        let gdrive_file: GdriveFile =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        if let Some(v) = gdrive_file.size {
            meta.set_content_length(v.parse::<u64>().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "parse content length").set_source(e)
            })?);
        }
        if let Some(v) = gdrive_file.modified_time {
            meta.set_last_modified(v.parse::<chrono::DateTime<Utc>>().map_err(|e| {
                Error::new(ErrorKind::Unexpected, "parse last modified time").set_source(e)
            })?);
        }
    };

    Ok(meta)
}

impl GdriveLister {
    pub fn new(path: String, core: Arc<GdriveCore>, op: OpList) -> Self {
        Self { path, core, op }
    }
}

impl oio::PageList for GdriveLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let file_id = self.core.path_cache.get(&self.path).await?;

        let file_id = match file_id {
            Some(file_id) => file_id,
            None => {
                ctx.done = true;
                return Ok(());
            }
        };

        let resp = self
            .core
            .gdrive_list(file_id.as_str(), 100, &ctx.token)
            .await?;

        let bytes = match resp.status() {
            StatusCode::OK => resp.into_body().to_bytes(),
            _ => return Err(parse_error(resp)),
        };

        // Gdrive returns empty content when this dir is not exist.
        if bytes.is_empty() {
            ctx.done = true;
            return Ok(());
        }

        // Return self at the first page.
        if ctx.token.is_empty() && !ctx.done {
            let path = build_rel_path(&self.core.root, &self.path);
            let meta = get_meta(self.core.clone(), EntryMode::DIR, self.op.metakey(), &path).await?;
            let e = oio::Entry::new(&path, meta);
            ctx.entries.push_back(e);
        }

        let decoded_response =
            serde_json::from_slice::<GdriveFileList>(&bytes).map_err(new_json_deserialize_error)?;

        if let Some(next_page_token) = decoded_response.next_page_token {
            ctx.token = next_page_token;
        } else {
            ctx.done = true;
        }

        let executor = self.op.executor().cloned().unwrap_or_default();
        let mut tasks = ConcurrentTasks::new(
            executor,
            self.op.concurrent(),
            |stat_handle: GdriveStatHandle| {
            Box::pin({
                async move {
                    match get_meta(stat_handle.core.clone(), stat_handle.file_type, stat_handle.metakey, &stat_handle.path).await {
                        Ok(meta) => {
                            let entry = oio::Entry::new(stat_handle.path.as_str(), meta);
                            (stat_handle, Ok(entry))
                        },
                        Err(err) => (stat_handle, Err(err)),
                    }
                }
            })
        });

        for mut file in decoded_response.files {
            let file_type = if file.mime_type.as_str() == "application/vnd.google-apps.folder" {
                if !file.name.ends_with('/') {
                    file.name += "/";
                }
                EntryMode::DIR
            } else {
                EntryMode::FILE
            };

            let path = format!("{}{}", &self.path, file.name);
            
            // Update path cache with list result.
            //
            // Only cache non-existent entry. When Google Drive converts a format,
            // for example, Microsoft Powerpoint, they will be two entries.
            // These two entries have the same file id.
            if let Ok(None) = self.core.path_cache.get(&path).await {
                self.core.path_cache.insert(&path, &file.id).await;
            }

            let root = &self.core.root;
            let normalized_path = build_rel_path(root, &path);
            let request = GdriveStatHandle {
                core: self.core.clone(),
                metakey: self.op.metakey(),
                file_type,
                path: normalized_path
            };

            tasks.execute(request).await.map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "executor fails to execute the task",
                )
                .set_source(err)
            });
        }

        loop {
            match tasks.next().await.transpose() {
                Ok(Some(entry)) => ctx.entries.push_back(entry),
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        Ok(())
    }
}
