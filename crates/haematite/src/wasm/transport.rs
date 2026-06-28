//! Browser WebSocket transport for haematite distribution sync (WASM-003, R1/R2).
//!
//! This module carries the *exact same* sync-protocol frames that the native
//! distribution transport (DIST-001) writes over TCP — only the carrier differs.
//! A native node frames a [`crate::sync::SyncMessage`] with
//! [`crate::sync::encode_beamr_sync_frame`] and writes those bytes onto a
//! `DistConnection`; the browser does the identical thing, except the bytes ride
//! a `web_sys::WebSocket` *binary* frame instead. There is **no** browser-specific
//! message format: the payload of a WebSocket binary frame is byte-for-byte the
//! frame the shared codec produced, so a native node and a WASM node speak the
//! same protocol over the wire (R2).
//!
//! # Layering
//! * Platform-neutral ([`frame`]): validate that a buffer is a well-formed sync
//!   control frame and hand its bytes to/from the carrier unchanged. This is the
//!   load-bearing parity surface and is exercised by native `#[test]`s below
//!   against the real [`crate::sync`] codec.
//! * Browser-only ([`socket`], `cfg(wasm32)`): a [`WebSocketSyncTransport`] that
//!   sets the socket to binary mode, pushes frame bytes as binary messages, and
//!   collects inbound binary frames for the caller to decode with the shared
//!   codec. Compiled only for `wasm32-unknown-unknown`; it cannot run in a
//!   headless host, so its live I/O is compile-gated, not natively executed.

/// Platform-neutral frame handling shared by the native parity tests and the
/// browser socket adapter.
///
/// Operates on opaque sync-frame bytes — the same bytes the native codec emits —
/// and never re-encodes the protocol.
pub mod frame {
    /// The control tag every haematite sync frame begins with.
    ///
    /// Mirrored from the native wire codec
    /// (`sync::protocol::wire::SYNC_CONTROL_FRAME`). A WebSocket binary frame
    /// whose payload does not start with this header (after the two big-endian
    /// `u32` length fields) is not a sync frame and is rejected, so a stray
    /// inbound message can never be mistaken for protocol traffic.
    pub const SYNC_CONTROL_TAG: &[u8] = b"haematite.sync.v1";

    /// Number of header bytes preceding the control tag: a `u32` control-tag
    /// length and a `u32` payload length, both big-endian, matching
    /// `wrap_beamr_sync_frame` on the native side.
    const FRAME_HEADER_BYTES: usize = 8;

    /// Error rejecting a buffer that is not a transportable sync frame.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum FrameError {
        /// The buffer is shorter than the fixed control-frame header.
        TooShort,
        /// The framed control tag is not the haematite sync tag, so the buffer is
        /// not sync-protocol traffic.
        UnexpectedControlTag,
        /// The control-tag length field disagrees with the haematite sync tag
        /// length, so the buffer cannot be a well-formed sync frame.
        ControlLengthMismatch,
    }

    impl core::fmt::Display for FrameError {
        fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            match self {
                Self::TooShort => formatter.write_str("buffer is shorter than a sync frame header"),
                Self::UnexpectedControlTag => {
                    formatter.write_str("binary frame is not haematite sync-protocol traffic")
                }
                Self::ControlLengthMismatch => {
                    formatter.write_str("sync frame control-tag length is malformed")
                }
            }
        }
    }

    impl core::error::Error for FrameError {}

    /// Read the big-endian `u32` at `offset` from `bytes`, if present.
    fn read_u32_be(bytes: &[u8], offset: usize) -> Option<u32> {
        let end = offset.checked_add(4)?;
        let slice = bytes.get(offset..end)?;
        let mut value = [0_u8; 4];
        value.copy_from_slice(slice);
        Some(u32::from_be_bytes(value))
    }

    /// Confirm `bytes` is a haematite sync control frame.
    ///
    /// It must be at least a full header long, its control-tag length field must
    /// equal the sync tag length, and the framed tag must be exactly
    /// [`SYNC_CONTROL_TAG`].
    ///
    /// This is the single validation the WebSocket carrier applies; it never
    /// parses the protocol payload (that is the shared codec's job). It exists so
    /// the carrier preserves the native frame byte-for-byte while rejecting
    /// non-protocol noise — the guarantee the parity tests pin down.
    ///
    /// # Errors
    /// Returns [`FrameError`] when `bytes` is too short, has a control-length
    /// field that disagrees with the sync tag, or carries a different tag.
    pub fn validate_sync_frame(bytes: &[u8]) -> Result<(), FrameError> {
        if bytes.len() < FRAME_HEADER_BYTES {
            return Err(FrameError::TooShort);
        }
        let control_len = read_u32_be(bytes, 0).ok_or(FrameError::TooShort)? as usize;
        if control_len != SYNC_CONTROL_TAG.len() {
            return Err(FrameError::ControlLengthMismatch);
        }
        let tag_start = FRAME_HEADER_BYTES;
        let tag_end = tag_start
            .checked_add(control_len)
            .ok_or(FrameError::TooShort)?;
        let tag = bytes.get(tag_start..tag_end).ok_or(FrameError::TooShort)?;
        if tag != SYNC_CONTROL_TAG {
            return Err(FrameError::UnexpectedControlTag);
        }
        Ok(())
    }

    /// Prepare an outbound sync frame for a WebSocket *binary* message.
    ///
    /// The WebSocket carrier ships protocol frames verbatim — the binary message
    /// payload IS the frame the shared codec produced — so this borrows the
    /// already-encoded frame after confirming it is genuinely a sync frame. No
    /// copy and no re-encoding: that byte-preservation is what makes a native
    /// node and a WASM node wire-compatible (R2).
    ///
    /// # Errors
    /// Returns [`FrameError`] if `frame` is not a haematite sync frame.
    pub fn outbound_binary_payload(frame: &[u8]) -> Result<&[u8], FrameError> {
        validate_sync_frame(frame)?;
        Ok(frame)
    }

    /// Accept an inbound WebSocket *binary* message as a sync frame.
    ///
    /// The bytes are returned unchanged for the caller to decode with the shared
    /// codec ([`crate::sync::decode_beamr_sync_frame`] on a native peer). This is
    /// the exact inverse of [`outbound_binary_payload`]: the carrier adds and
    /// removes nothing, so the frame the sender encoded is the frame the receiver
    /// decodes.
    ///
    /// # Errors
    /// Returns [`FrameError`] if `payload` is not a haematite sync frame.
    pub fn inbound_frame(payload: Vec<u8>) -> Result<Vec<u8>, FrameError> {
        validate_sync_frame(&payload)?;
        Ok(payload)
    }
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use socket::{WebSocketSyncTransport, WebSocketTransportError};

/// Browser WebSocket carrier. Compiled only for `wasm32-unknown-unknown`:
/// `web_sys::WebSocket` has no host-runnable equivalent, so the live send/recv
/// path is verified by `cargo check --target wasm32 --features wasm`, not by a
/// native test run.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod socket {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use js_sys::{ArrayBuffer, Uint8Array};
    use wasm_bindgen::JsCast;
    use wasm_bindgen::closure::Closure;
    use wasm_bindgen::prelude::*;
    use web_sys::{BinaryType, MessageEvent, WebSocket};

    use super::frame;

    /// Errors from the browser WebSocket carrier.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum WebSocketTransportError {
        /// The socket could not be opened (bad URL, blocked, etc.).
        Open(String),
        /// `WebSocket.send` failed (e.g. the socket is closing/closed).
        Send(String),
        /// An outbound buffer or inbound message was not a valid sync frame.
        Frame(frame::FrameError),
        /// An inbound message arrived as text, not the expected binary frame.
        NonBinaryMessage,
    }

    impl core::fmt::Display for WebSocketTransportError {
        fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            match self {
                Self::Open(message) => write!(formatter, "websocket open failed: {message}"),
                Self::Send(message) => write!(formatter, "websocket send failed: {message}"),
                Self::Frame(error) => write!(formatter, "websocket frame error: {error}"),
                Self::NonBinaryMessage => {
                    formatter.write_str("websocket message was text, expected a binary sync frame")
                }
            }
        }
    }

    impl core::error::Error for WebSocketTransportError {}

    impl From<frame::FrameError> for WebSocketTransportError {
        fn from(error: frame::FrameError) -> Self {
            Self::Frame(error)
        }
    }

    fn js_message(value: &JsValue) -> String {
        value
            .as_string()
            .or_else(|| js_sys::Error::from(value.clone()).message().as_string())
            .unwrap_or_else(|| String::from("unknown JavaScript error"))
    }

    /// A WebSocket adapter that carries haematite sync frames to and from a
    /// server endpoint.
    ///
    /// Inbound binary frames are validated as sync frames and queued; the caller
    /// drains them with [`Self::drain_inbound`] and decodes each with the shared
    /// codec (`crate::sync::decode_beamr_sync_frame`). The adapter holds the
    /// `onmessage` closure alive for the socket's lifetime.
    pub struct WebSocketSyncTransport {
        socket: WebSocket,
        inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
        _on_message: Closure<dyn FnMut(MessageEvent)>,
    }

    impl WebSocketSyncTransport {
        /// Connect to `endpoint` (a `ws://`/`wss://` URL) and begin queueing
        /// inbound binary sync frames.
        ///
        /// The socket is put in `arraybuffer` binary mode so inbound frames
        /// arrive as raw bytes rather than `Blob`s. Connection completion is
        /// asynchronous; the caller should wait for the socket `open` event (or
        /// simply send once `ready_state` reports `OPEN`) before driving a sync.
        ///
        /// # Errors
        /// Returns [`WebSocketTransportError::Open`] if the socket cannot be
        /// constructed.
        pub fn connect(endpoint: &str) -> Result<Self, WebSocketTransportError> {
            let socket = WebSocket::new(endpoint)
                .map_err(|error| WebSocketTransportError::Open(js_message(&error)))?;
            socket.set_binary_type(BinaryType::Arraybuffer);

            let inbound: Rc<RefCell<VecDeque<Vec<u8>>>> = Rc::new(RefCell::new(VecDeque::new()));
            let inbound_for_cb = Rc::clone(&inbound);
            let on_message = Closure::wrap(Box::new(move |event: MessageEvent| {
                let data = event.data();
                if let Some(buffer) = data.dyn_ref::<ArrayBuffer>() {
                    let bytes = Uint8Array::new(buffer).to_vec();
                    if let Ok(valid) = frame::inbound_frame(bytes) {
                        inbound_for_cb.borrow_mut().push_back(valid);
                    }
                }
            }) as Box<dyn FnMut(MessageEvent)>);
            socket.set_onmessage(Some(on_message.as_ref().unchecked_ref()));

            Ok(Self {
                socket,
                inbound,
                _on_message: on_message,
            })
        }

        /// Send one already-encoded sync frame as a binary WebSocket message.
        ///
        /// `frame` must be the output of the shared codec
        /// (`crate::sync::encode_beamr_sync_frame`); it is shipped byte-for-byte
        /// so a native peer decodes the identical message (R2).
        ///
        /// # Errors
        /// [`WebSocketTransportError::Frame`] if `frame` is not a sync frame, or
        /// [`WebSocketTransportError::Send`] if the socket rejects the write.
        pub fn send_frame(&self, frame_bytes: &[u8]) -> Result<(), WebSocketTransportError> {
            let payload = frame::outbound_binary_payload(frame_bytes)?;
            self.socket
                .send_with_u8_array(payload)
                .map_err(|error| WebSocketTransportError::Send(js_message(&error)))
        }

        /// Drive a pull-sync handshake by sending the caller-encoded pull request
        /// frame. The response frames arrive asynchronously via `onmessage` and
        /// are retrieved with [`Self::drain_inbound`]; this method only kicks the
        /// exchange off (the sync protocol itself is DIST-001).
        ///
        /// # Errors
        /// As [`Self::send_frame`].
        pub fn start_pull_sync(
            &self,
            pull_request_frame: &[u8],
        ) -> Result<(), WebSocketTransportError> {
            self.send_frame(pull_request_frame)
        }

        /// Take all inbound sync frames received so far. Each element is a
        /// complete frame ready for `crate::sync::decode_beamr_sync_frame`.
        pub fn drain_inbound(&self) -> Vec<Vec<u8>> {
            self.inbound.borrow_mut().drain(..).collect()
        }

        /// Number of inbound frames queued but not yet drained.
        pub fn pending_inbound(&self) -> usize {
            self.inbound.borrow().len()
        }

        /// The underlying socket's `readyState` (`CONNECTING`/`OPEN`/`CLOSING`/
        /// `CLOSED`).
        pub fn ready_state(&self) -> u16 {
            self.socket.ready_state()
        }

        /// Borrow the underlying socket (e.g. to await `open`/`close` events).
        pub const fn socket(&self) -> &WebSocket {
            &self.socket
        }
    }
}

#[cfg(test)]
mod tests {
    //! Parity tests (R2). They prove the WebSocket carrier preserves the shared
    //! sync codec's frames byte-for-byte: a message encoded by
    //! [`crate::sync_codec::encode_beamr_sync_frame`] survives the carrier's
    //! outbound and inbound frame handling unchanged and decodes back to an
    //! identical [`crate::sync_codec::SyncMessage`]. No WebSocket is involved — the
    //! carrier's frame functions ARE the parity surface. They import the codec from
    //! the ungated `crate::sync_codec` so they compile and run on BOTH the native
    //! host and the wasm target.

    use super::frame;
    use crate::sync_codec::ballot::Ballot;
    use crate::sync_codec::{
        NodeTransfer, PullRequest, PushResponse, RootExchangeRequest, SyncMessage, SyncStats,
        WriteId, WriteProposal, decode_beamr_sync_frame, encode_beamr_sync_frame,
    };
    use crate::tree::{LeafNode, Node};

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn leaf(key: &[u8], value: &[u8]) -> Result<Node, Box<dyn std::error::Error>> {
        Ok(Node::Leaf(LeafNode::new(vec![(
            key.to_vec(),
            value.to_vec(),
        )])?))
    }

    fn sample_messages() -> Result<Vec<SyncMessage>, Box<dyn std::error::Error>> {
        let transfer = NodeTransfer::new(leaf(b"alpha", b"one")?);
        let push = PushResponse::new(0, None, None, vec![transfer], SyncStats::default());
        Ok(vec![
            SyncMessage::RootRequest(RootExchangeRequest::new(3, None)),
            SyncMessage::PullRequest(PullRequest::new(1, None)),
            SyncMessage::PushResponse(push),
            SyncMessage::WriteProposal(WriteProposal {
                write_id: WriteId::new("node-a", 7, 42),
                shard_id: 2,
                key: b"k".to_vec(),
                expected: None,
                value: b"v".to_vec(),
                ttl: None,
                epoch: Ballot::bottom(),
                seq: 0,
                tombstone: false,
            }),
        ])
    }

    /// R2: a frame produced by the shared codec is accepted by the carrier's
    /// outbound path verbatim — no bytes added, removed, or reordered.
    #[test]
    fn outbound_payload_is_the_codec_frame_verbatim() -> TestResult {
        for message in sample_messages()? {
            let frame_bytes = encode_beamr_sync_frame(&message)?;
            let payload = frame::outbound_binary_payload(&frame_bytes)?;
            assert_eq!(
                payload, frame_bytes,
                "carrier must ship the exact codec frame bytes"
            );
        }
        Ok(())
    }

    /// R2 (load-bearing): a message -> codec frame -> carrier outbound -> carrier
    /// inbound -> codec decode round-trips to an identical message. This is the
    /// proof a native node and a WASM node are byte-compatible: the only thing
    /// between encode and decode is the carrier, and it changes nothing.
    #[test]
    fn frame_round_trips_through_carrier_to_identical_message() -> TestResult {
        for message in sample_messages()? {
            let frame_bytes = encode_beamr_sync_frame(&message)?;

            // Outbound: what the WASM sender hands to WebSocket.send.
            let on_wire = frame::outbound_binary_payload(&frame_bytes)?.to_vec();

            // Inbound: what a peer receives from its WebSocket onmessage.
            let received = frame::inbound_frame(on_wire)?;

            // The native peer decodes with the SAME shared codec.
            let decoded = decode_beamr_sync_frame(&received)?;
            assert_eq!(decoded, message, "round-trip must preserve the message");
        }
        Ok(())
    }

    /// R2: the bytes a WASM sender would put on the wire are exactly the bytes a
    /// native node writes for the same message, so root-hash exchange, tree-walk,
    /// and node-transfer messages are byte-identical across builds.
    #[test]
    fn wasm_wire_bytes_match_native_codec_bytes() -> TestResult {
        let message = SyncMessage::PushResponse(PushResponse::new(
            0,
            None,
            None,
            vec![NodeTransfer::new(leaf(b"k", b"v")?)],
            SyncStats::default(),
        ));
        let native_frame = encode_beamr_sync_frame(&message)?;
        let wasm_wire = frame::outbound_binary_payload(&native_frame)?.to_vec();
        assert_eq!(wasm_wire, native_frame);
        // And the same bytes decode back on the native side.
        let decoded = decode_beamr_sync_frame(&wasm_wire)?;
        assert_eq!(decoded, message);
        Ok(())
    }

    /// The carrier rejects a buffer that is not a haematite sync frame, so stray
    /// inbound traffic is never misread as protocol.
    #[test]
    fn validate_rejects_non_sync_frames() {
        assert_eq!(
            frame::validate_sync_frame(&[]),
            Err(frame::FrameError::TooShort)
        );
        assert_eq!(
            frame::validate_sync_frame(&[0, 0, 0, 0, 0, 0, 0, 0]),
            Err(frame::FrameError::ControlLengthMismatch)
        );
        // Right control length, wrong tag (17-byte tag, matching the sync tag len).
        let mut wrong_tag = Vec::new();
        wrong_tag.extend_from_slice(&(frame::SYNC_CONTROL_TAG.len() as u32).to_be_bytes());
        wrong_tag.extend_from_slice(&0_u32.to_be_bytes());
        wrong_tag.extend_from_slice(b"not-a-sync-tag-xx");
        assert_eq!(
            frame::validate_sync_frame(&wrong_tag),
            Err(frame::FrameError::UnexpectedControlTag)
        );
    }

    /// A real codec frame passes validation (the positive companion to the
    /// rejection test).
    #[test]
    fn validate_accepts_a_real_codec_frame() -> TestResult {
        let frame_bytes =
            encode_beamr_sync_frame(&SyncMessage::PullRequest(PullRequest::new(0, None)))?;
        assert_eq!(frame::validate_sync_frame(&frame_bytes), Ok(()));
        Ok(())
    }
}
