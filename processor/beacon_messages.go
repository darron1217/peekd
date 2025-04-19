package processor

import (
	ethtypes "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
)

type BeaconMessageMetadata struct {
	PeerID  string `json:"PeerID"`
	Topic   string `json:"Topic"`
	Seq     []byte `json:"Seq"`
	MsgID   string `json:"MsgID"`
	MsgSize int    `json:"MsgSize"`
}

type Phase0BlockMessage struct {
	Metadata BeaconMessageMetadata
	Block    *ethtypes.SignedBeaconBlock
}

type AltairBlockMessage struct {
	Metadata BeaconMessageMetadata
	Block    *ethtypes.SignedBeaconBlockAltair
}

type BellatrixBlockMessage struct {
	Metadata BeaconMessageMetadata
	Block    *ethtypes.SignedBeaconBlockBellatrix
}

type CapellaBlockMessage struct {
	Metadata BeaconMessageMetadata
	Block    *ethtypes.SignedBeaconBlockCapella
}

type DenebBlockMessage struct {
	Metadata BeaconMessageMetadata
	Block    *ethtypes.SignedBeaconBlockDeneb
}

type ElectraBlockMessage struct {
	Metadata BeaconMessageMetadata
	Block    *ethtypes.SignedBeaconBlockElectra
}

type BlobSidecarMessage struct {
	Metadata BeaconMessageMetadata
	Blob     *ethtypes.BlobSidecar
}

type AttestationMessage struct {
	Metadata    BeaconMessageMetadata
	Attestation *ethtypes.Attestation
}

type AttestationElectraMessage struct {
	Metadata    BeaconMessageMetadata
	Attestation *ethtypes.AttestationElectra
}

type AggregateAttestationAndProofMessage struct {
	Metadata                     BeaconMessageMetadata
	AggregateAttestationAndProof *ethtypes.SignedAggregateAttestationAndProof
}

type AggregateAttestationAndProofElectraMessage struct {
	Metadata                     BeaconMessageMetadata
	AggregateAttestationAndProof *ethtypes.SignedAggregateAttestationAndProofElectra
}

type SyncCommitteeMessage struct {
	Metadata      BeaconMessageMetadata
	SyncCommittee *ethtypes.SyncCommitteeMessage
}

type ContributionAndProofMessage struct {
	Metadata             BeaconMessageMetadata
	ContributionAndProof *ethtypes.SignedContributionAndProof
}

type ProposerSlashingMessage struct {
	Metadata         BeaconMessageMetadata
	ProposerSlashing *ethtypes.ProposerSlashing
}

type AttesterSlashingMessage struct {
	Metadata         BeaconMessageMetadata
	AttesterSlashing *ethtypes.AttesterSlashing
}

type AttesterSlashingElectraMessage struct {
	Metadata         BeaconMessageMetadata
	AttesterSlashing *ethtypes.AttesterSlashingElectra
}

type VoluntaryExitMessage struct {
	Metadata      BeaconMessageMetadata
	VoluntaryExit *ethtypes.VoluntaryExit
}

type BLSToExecutionChangeMessage struct {
	Metadata             BeaconMessageMetadata
	BLSToExecutionChange *ethtypes.BLSToExecutionChange
}

type SingleAttestationMessage struct {
	Metadata          BeaconMessageMetadata
	SingleAttestation *ethtypes.SingleAttestation
}

type SyncCommitteeContributionMessage struct {
	Metadata                  BeaconMessageMetadata
	SyncCommitteeContribution *ethtypes.SyncCommitteeContribution
}

type SyncCommitteeContributionAndProofMessage struct {
	Metadata                          BeaconMessageMetadata
	SyncCommitteeContributionAndProof *ethtypes.SignedContributionAndProof
}
