package ids

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestMultiNamespaceIdEncodeDecode(t *testing.T) {
	d := &MultiTenantIDsEncoder{}

	for range 10000 {
		id := &corepb.NamespaceId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
		}

		actual, err := d.DecodeNamespaceId(d.EncodeNamespaceId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestMultiNamespaceIdDecode(t *testing.T) {
	d := &MultiTenantIDsEncoder{}

	_, err := d.DecodeNamespaceId("err_Gymvy7sGiJ8HZikl0a5PeF")
	require.Error(t, err)

	_, err = d.DecodeNamespaceId("ns_Gymvy7sGiJ8kl0a5PeF")
	require.Error(t, err)

	_, err = d.DecodeNamespaceId("ns_Gymvy7sGiJ8H2Zikl0a5PsheF")
	require.Error(t, err)

	_, err = d.DecodeNamespaceId("ns_Gymvy7sGiJ8HZ+kl0a5PeF")
	require.Error(t, err)

	_, err = d.DecodeNamespaceId("prens_Gymvy7sGiJ8HZikl0a5PeF")
	require.Error(t, err)

	_, err = d.DecodeNamespaceId("ns_Gymvy7sGiJ8HZikl0a5PeF")
	require.NoError(t, err)
}

func TestMultiWaitGroupIdEncodeDecode(t *testing.T) {
	d := &MultiTenantIDsEncoder{}

	for range 10000 {
		id := &corepb.WaitGroupId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
			WaitGroupId: rand.Uint64(),
		}

		actual, err := d.DecodeWaitGroupId(d.EncodeWaitGroupId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestMultiWaitGroupIdDecode(t *testing.T) {
	d := &MultiTenantIDsEncoder{}

	_, err := d.DecodeWaitGroupId("err_QWWpHwPMd7vergOWga59bqcEY35eSJbqD")
	require.Error(t, err)

	_, err = d.DecodeWaitGroupId("wg_QWWpHwPMd7vergOWga59bqcEY35ezdf4SJbqD")
	require.Error(t, err)

	_, err = d.DecodeWaitGroupId("wg_QWWpHwPMd7vergOWga59bqcEY35eSJ")
	require.Error(t, err)

	_, err = d.DecodeWaitGroupId("wg_QWWpHwPMd7vergOWga59bqcE+35eSJbqD")
	require.Error(t, err)

	_, err = d.DecodeWaitGroupId("prewg_QWWpHwPMd7vergOWga59bqcEY35eSJbqD")
	require.Error(t, err)

	_, err = d.DecodeWaitGroupId("wg_QWWpHwPMd7vergOWga59bqcEY35eSJbqD")
	require.NoError(t, err)
}

func TestMultiSemaphoreIdEncodeDecode(t *testing.T) {
	d := &MultiTenantIDsEncoder{}

	for range 10000 {
		id := &corepb.SemaphoreId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
			SemaphoreId: rand.Uint64(),
		}

		actual, err := d.DecodeSemaphoreId(d.EncodeSemaphoreId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestMultiSemaphoreIdDecode(t *testing.T) {
	d := &MultiTenantIDsEncoder{}

	_, err := d.DecodeSemaphoreId("err_RzcSzFQKbJMxlRB69887YW64aoSVbetGB")
	require.Error(t, err)

	_, err = d.DecodeSemaphoreId("sem_RzcSzFQKbJMxlRB69887YW64aoSVbe")
	require.Error(t, err)

	_, err = d.DecodeSemaphoreId("sem_RzcSzFQKbJMxlRB69887YW64aoSVbesd6tGB")
	require.Error(t, err)

	_, err = d.DecodeSemaphoreId("sem_RzcSzFQKbJMxlRB69887YW6+aoSVbetGB")
	require.Error(t, err)

	_, err = d.DecodeSemaphoreId("presem_RzcSzFQKbJMxlRB69887YW64aoSVbetGB")
	require.Error(t, err)

	_, err = d.DecodeSemaphoreId("sem_RzcSzFQKbJMxlRB69887YW64aoSVbetGB")
	require.NoError(t, err)
}

func TestMultiBarrierIdEncodeDecode(t *testing.T) {
	d := &MultiTenantIDsEncoder{}

	for range 10000 {
		id := &corepb.BarrierId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
			BarrierId:   rand.Uint64(),
		}

		actual, err := d.DecodeBarrierId(d.EncodeBarrierId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestMultiBarrierIdDecode(t *testing.T) {
	d := &MultiTenantIDsEncoder{}

	_, err := d.DecodeBarrierId("err_ex2TSzfUOrbQG7PehYpmXHhqAI8ye9qrC")
	require.Error(t, err)

	_, err = d.DecodeBarrierId("bar_ex2TSzfUOrbQG7PehYpmXHhqAI8ye9")
	require.Error(t, err)

	_, err = d.DecodeBarrierId("bar_ex2TSzfUOrbQG7PehYpmXHhqAI8yea4f9qrC")
	require.Error(t, err)

	_, err = d.DecodeBarrierId("bar_ex2TSzfUOrbQG7PehYpmXHhqA+8ye9qrC")
	require.Error(t, err)

	_, err = d.DecodeBarrierId("prebar_ex2TSzfUOrbQG7PehYpmXHhqAI8ye9qrC")
	require.Error(t, err)

	_, err = d.DecodeBarrierId("bar_ex2TSzfUOrbQG7PehYpmXHhqAI8ye9qrC")
	require.NoError(t, err)
}

func TestMultiLeaseIdEncodeDecode(t *testing.T) {
	d := &MultiTenantIDsEncoder{}

	for range 10000 {
		id := &corepb.LeaseId{
			AccountId:   rand.Uint64(),
			NamespaceId: rand.Uint64(),
			LeaseId:     rand.Uint64(),
		}

		actual, err := d.DecodeLeaseId(d.EncodeLeaseId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestMultiLeaseIdDecode(t *testing.T) {
	d := &MultiTenantIDsEncoder{}

	_, err := d.DecodeLeaseId("err_1fM5oldgzaB3TfUzFNzQfMP8ek3XbnFQE")
	require.Error(t, err)

	_, err = d.DecodeLeaseId("ls_1fM5oldgzaB3TfUzFNzQfMP8ek3Xbn")
	require.Error(t, err)

	_, err = d.DecodeLeaseId("ls_1fM5oldgzaB3TfUzFNzQfMP8ek3Xbnad4FQE")
	require.Error(t, err)

	_, err = d.DecodeLeaseId("ls_1fM5oldgzaB3TfUzFNzQfMPe+k3XbnFQE")
	require.Error(t, err)

	_, err = d.DecodeLeaseId("prels_1fM5oldgzaB3TfUzFNzQfMP8ek3XbnFQE")
	require.Error(t, err)

	_, err = d.DecodeLeaseId("ls_1fM5oldgzaB3TfUzFNzQfMP8ek3XbnFQE")
	require.NoError(t, err)
}
