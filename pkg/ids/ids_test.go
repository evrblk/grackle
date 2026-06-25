package ids

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestNamespaceIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}

		actual, err := DecodeNamespaceId(EncodeNamespaceId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestNamespaceIdDecode(t *testing.T) {
	_, err := DecodeNamespaceId("err_Gymvy7sGiJ8HZikl0a5PeF")
	require.Error(t, err)

	_, err = DecodeNamespaceId("ns_Gymvy7sGiJ8kl0a5PeF")
	require.Error(t, err)

	_, err = DecodeNamespaceId("ns_Gymvy7sGiJ8H2Zikl0a5PsheF")
	require.Error(t, err)

	_, err = DecodeNamespaceId("ns_Gymvy7sGiJ8HZ+kl0a5PeF")
	require.Error(t, err)

	_, err = DecodeNamespaceId("prens_Gymvy7sGiJ8HZikl0a5PeF")
	require.Error(t, err)

	_, err = DecodeNamespaceId("ns_Gymvy7sGiJ8HZikl0a5PeF")
	require.NoError(t, err)
}

func TestWaitGroupIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
			WaitGroupId: rand.Uint64(),
		}

		actual, err := DecodeWaitGroupId(EncodeWaitGroupId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestWaitGroupIdDecode(t *testing.T) {
	_, err := DecodeWaitGroupId("err_QWWpHwPMd7vergOWga59bqcEY35eSJbqD")
	require.Error(t, err)

	_, err = DecodeWaitGroupId("wg_QWWpHwPMd7vergOWga59bqcEY35ezdf4SJbqD")
	require.Error(t, err)

	_, err = DecodeWaitGroupId("wg_QWWpHwPMd7vergOWga59bqcEY35eSJ")
	require.Error(t, err)

	_, err = DecodeWaitGroupId("wg_QWWpHwPMd7vergOWga59bqcE+35eSJbqD")
	require.Error(t, err)

	_, err = DecodeWaitGroupId("prewg_QWWpHwPMd7vergOWga59bqcEY35eSJbqD")
	require.Error(t, err)

	_, err = DecodeWaitGroupId("wg_QWWpHwPMd7vergOWga59bqcEY35eSJbqD")
	require.NoError(t, err)
}

func TestSemaphoreIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.SemaphoreId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
			SemaphoreId: rand.Uint64(),
		}

		actual, err := DecodeSemaphoreId(EncodeSemaphoreId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestSemaphoreIdDecode(t *testing.T) {
	_, err := DecodeSemaphoreId("err_RzcSzFQKbJMxlRB69887YW64aoSVbetGB")
	require.Error(t, err)

	_, err = DecodeSemaphoreId("sem_RzcSzFQKbJMxlRB69887YW64aoSVbe")
	require.Error(t, err)

	_, err = DecodeSemaphoreId("sem_RzcSzFQKbJMxlRB69887YW64aoSVbesd6tGB")
	require.Error(t, err)

	_, err = DecodeSemaphoreId("sem_RzcSzFQKbJMxlRB69887YW6+aoSVbetGB")
	require.Error(t, err)

	_, err = DecodeSemaphoreId("presem_RzcSzFQKbJMxlRB69887YW64aoSVbetGB")
	require.Error(t, err)

	_, err = DecodeSemaphoreId("sem_RzcSzFQKbJMxlRB69887YW64aoSVbetGB")
	require.NoError(t, err)
}

func TestBarrierIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
			BarrierId:   rand.Uint64(),
		}

		actual, err := DecodeBarrierId(EncodeBarrierId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestBarrierIdDecode(t *testing.T) {
	_, err := DecodeBarrierId("err_ex2TSzfUOrbQG7PehYpmXHhqAI8ye9qrC")
	require.Error(t, err)

	_, err = DecodeBarrierId("bar_ex2TSzfUOrbQG7PehYpmXHhqAI8ye9")
	require.Error(t, err)

	_, err = DecodeBarrierId("bar_ex2TSzfUOrbQG7PehYpmXHhqAI8yea4f9qrC")
	require.Error(t, err)

	_, err = DecodeBarrierId("bar_ex2TSzfUOrbQG7PehYpmXHhqA+8ye9qrC")
	require.Error(t, err)

	_, err = DecodeBarrierId("prebar_ex2TSzfUOrbQG7PehYpmXHhqAI8ye9qrC")
	require.Error(t, err)

	_, err = DecodeBarrierId("bar_ex2TSzfUOrbQG7PehYpmXHhqAI8ye9qrC")
	require.NoError(t, err)
}

func TestLeaseIdEncodeDecode(t *testing.T) {
	for range 10000 {
		id := &corepb.LeaseId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
			LeaseId:     rand.Uint64(),
		}

		actual, err := DecodeLeaseId(EncodeLeaseId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestLeaseIdDecode(t *testing.T) {
	_, err := DecodeLeaseId("err_1fM5oldgzaB3TfUzFNzQfMP8ek3XbnFQE")
	require.Error(t, err)

	_, err = DecodeLeaseId("ls_1fM5oldgzaB3TfUzFNzQfMP8ek3Xbn")
	require.Error(t, err)

	_, err = DecodeLeaseId("ls_1fM5oldgzaB3TfUzFNzQfMP8ek3Xbnad4FQE")
	require.Error(t, err)

	_, err = DecodeLeaseId("ls_1fM5oldgzaB3TfUzFNzQfMPe+k3XbnFQE")
	require.Error(t, err)

	_, err = DecodeLeaseId("prels_1fM5oldgzaB3TfUzFNzQfMP8ek3XbnFQE")
	require.Error(t, err)

	_, err = DecodeLeaseId("ls_1fM5oldgzaB3TfUzFNzQfMP8ek3XbnFQE")
	require.NoError(t, err)
}
