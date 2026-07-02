pub fn split_chunks(body: &[u8], count: u8, threshold: usize) -> Option<Vec<&[u8]>> {
    if body.len() <= threshold {
        return None;
    }

    let chunk_size = body.len().div_ceil(count as usize);
    let chunks: Vec<&[u8]> = body.chunks(chunk_size).collect();
    Some(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_chunks_below_threshold() {
        let result = split_chunks(b"small", 3, 100);
        assert!(result.is_none());
    }

    #[test]
    fn test_split_chunks_at_threshold() {
        let result = split_chunks(b"exact", 3, 5);
        assert!(result.is_none());
    }

    #[test]
    fn test_split_chunks_above_threshold() {
        let result = split_chunks(b"large_body_data_here", 2, 5);
        assert!(result.is_some());
        let chunks = result.unwrap();
        assert_eq!(chunks.len(), 2);
        // Both chunks concatenated should equal original
        let rebuilt: Vec<u8> = chunks.iter().flat_map(|c| c.to_vec()).collect();
        assert_eq!(rebuilt, b"large_body_data_here");
    }

    #[test]
    fn test_split_chunks_exact_division() {
        let result = split_chunks(b"1234567890", 2, 5);
        assert!(result.is_some());
        let chunks = result.unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0], b"12345");
        assert_eq!(chunks[1], b"67890");
    }

    #[test]
    fn test_split_chunks_uneven_division() {
        let result = split_chunks(b"123456789", 4, 1);
        assert!(result.is_some());
        let chunks = result.unwrap();
        // len=9, ceil(9/4)=3, so chunks of 3: [123,456,789]
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], b"123");
        assert_eq!(chunks[1], b"456");
        assert_eq!(chunks[2], b"789");
    }

    #[test]
    fn test_split_chunks_single_chunk_when_count_larger() {
        let result = split_chunks(b"hello", 10, 2);
        assert!(result.is_some());
        let chunks = result.unwrap();
        // ceil(5/10)=1, chunk_size=1, so 5 chunks of 1 byte each
        assert_eq!(chunks.len(), 5);
        assert_eq!(chunks[0], b"h");
        assert_eq!(chunks[4], b"o");
    }

    #[test]
    fn test_split_chunks_empty_body() {
        let result = split_chunks(b"", 2, 0);
        assert!(result.is_none());
    }
}
