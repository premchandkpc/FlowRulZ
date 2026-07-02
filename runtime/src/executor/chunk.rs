pub fn split_chunks(body: &[u8], count: u8, threshold: usize) -> Option<Vec<&[u8]>> {
    if body.len() <= threshold {
        return None;
    }

    let chunk_size = body.len().div_ceil(count as usize);
    let chunks: Vec<&[u8]> = body.chunks(chunk_size).collect();
    Some(chunks)
}
