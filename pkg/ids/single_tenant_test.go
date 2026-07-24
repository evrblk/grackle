package ids

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/evrblk/grackle/pkg/corepb"
)

func TestSingleNamespaceIdEncodeDecode(t *testing.T) {
	d := &SingleTenantIDsEncoder{}

	for range 10000 {
		id := &corepb.NamespaceId{
			AccountId:   0,
			NamespaceId: rand.Uint64(),
		}

		actual, err := d.DecodeNamespaceId(d.EncodeNamespaceId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestSingleNamespaceIdDecode(t *testing.T) {
	d := &SingleTenantIDsEncoder{}

	_, err := d.DecodeNamespaceId("err_vzD6GjlCxWK")
	require.Error(t, err)

	_, err = d.DecodeNamespaceId("ns_vzD6GjlCxWKsr")
	require.Error(t, err)

	_, err = d.DecodeNamespaceId("ns_vzD6GjlCx")
	require.Error(t, err)

	_, err = d.DecodeNamespaceId("ns_vzD6Gj+lCxK")
	require.Error(t, err)

	_, err = d.DecodeNamespaceId("prens_vzD6GjlCxWK")
	require.Error(t, err)

	_, err = d.DecodeNamespaceId("ns_vzD6GjlCxWK")
	require.NoError(t, err)
}

func TestSingleWaitGroupIdEncodeDecode(t *testing.T) {
	d := &SingleTenantIDsEncoder{}

	for range 10000 {
		id := &corepb.WaitGroupId{
			AccountId:   0,
			NamespaceId: rand.Uint64(),
			WaitGroupId: rand.Uint64(),
		}

		actual, err := d.DecodeWaitGroupId(d.EncodeWaitGroupId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestSingleWaitGroupIdDecode(t *testing.T) {
	d := &SingleTenantIDsEncoder{}

	_, err := d.DecodeWaitGroupId("err_FIcSgfjfXBptS4LqWvVLLC")
	require.Error(t, err)

	_, err = d.DecodeWaitGroupId("wg_FIcSgfjfXBptS4LqWvVLLCsd4")
	require.Error(t, err)

	_, err = d.DecodeWaitGroupId("wg_FIcSgfjfXBptS4LqWLLC")
	require.Error(t, err)

	_, err = d.DecodeWaitGroupId("wg_FIcSgfjfXBptS4L+qWvVLC")
	require.Error(t, err)

	_, err = d.DecodeWaitGroupId("prewg_FIcSgfjfXBptS4LqWvVLLC")
	require.Error(t, err)

	_, err = d.DecodeWaitGroupId("wg_FIcSgfjfXBptS4LqWvVLLC")
	require.NoError(t, err)
}

func TestSingleSemaphoreIdEncodeDecode(t *testing.T) {
	d := &SingleTenantIDsEncoder{}

	for range 10000 {
		id := &corepb.SemaphoreId{
			AccountId:   0,
			NamespaceId: rand.Uint64(),
			SemaphoreId: rand.Uint64(),
		}

		actual, err := d.DecodeSemaphoreId(d.EncodeSemaphoreId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestSingleSemaphoreIdDecode(t *testing.T) {
	d := &SingleTenantIDsEncoder{}

	_, err := d.DecodeSemaphoreId("err_ym92RuccnkQQAkMs85lpwA")
	require.Error(t, err)

	_, err = d.DecodeSemaphoreId("sem_ym92RuccnkQQA85lpwA")
	require.Error(t, err)

	_, err = d.DecodeSemaphoreId("sem_ym92RuccnkQQAkMs85lpsd2wA")
	require.Error(t, err)

	_, err = d.DecodeSemaphoreId("sem_ym92RuccnkQ+AkMs85lpwA")
	require.Error(t, err)

	_, err = d.DecodeSemaphoreId("presem_ym92RuccnkQQAkMs85lpwA")
	require.Error(t, err)

	_, err = d.DecodeSemaphoreId("sem_ym92RuccnkQQAkMs85lpwA")
	require.NoError(t, err)
}

func TestSingleBarrierIdEncodeDecode(t *testing.T) {
	d := &SingleTenantIDsEncoder{}

	for range 10000 {
		id := &corepb.BarrierId{
			AccountId:   0,
			NamespaceId: rand.Uint64(),
			BarrierId:   rand.Uint64(),
		}

		actual, err := d.DecodeBarrierId(d.EncodeBarrierId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestSingleBarrierIdDecode(t *testing.T) {
	d := &SingleTenantIDsEncoder{}

	_, err := d.DecodeBarrierId("err_uqKokye9fpHzV3OtHsaTsE")
	require.Error(t, err)

	_, err = d.DecodeBarrierId("bar_uqKokye9fpHzVHsaTsE")
	require.Error(t, err)

	_, err = d.DecodeBarrierId("bar_uqKokye9fpHzV3Otsd5HsaTsE")
	require.Error(t, err)

	_, err = d.DecodeBarrierId("bar_uqKokye9fpHzV3O+HsaTsE")
	require.Error(t, err)

	_, err = d.DecodeBarrierId("prebar_uqKokye9fpHzV3OtHsaTsE")
	require.Error(t, err)

	_, err = d.DecodeBarrierId("bar_uqKokye9fpHzV3OtHsaTsE")
	require.NoError(t, err)
}

func TestSingleLeaseIdEncodeDecode(t *testing.T) {
	d := &SingleTenantIDsEncoder{}

	for range 10000 {
		id := &corepb.LeaseId{
			AccountId:   0,
			NamespaceId: rand.Uint64(),
			LeaseId:     rand.Uint64(),
		}

		actual, err := d.DecodeLeaseId(d.EncodeLeaseId(id))
		require.NoError(t, err)
		require.EqualValues(t, id, actual)
	}
}

func TestSingleLeaseIdDecode(t *testing.T) {
	d := &SingleTenantIDsEncoder{}

	_, err := d.DecodeLeaseId("err_osnQVCFZRZsEf63s9FS4kG")
	require.Error(t, err)

	_, err = d.DecodeLeaseId("ls_osnQVCFZRZ63s9FS4kG")
	require.Error(t, err)

	_, err = d.DecodeLeaseId("ls_osnQVCFZRZsEf63shd69FS4kG")
	require.Error(t, err)

	_, err = d.DecodeLeaseId("ls_osnQVCFZRZsEf6+s9FS4kG")
	require.Error(t, err)

	_, err = d.DecodeLeaseId("prels_osnQVCFZRZsEf63s9FS4kG")
	require.Error(t, err)

	_, err = d.DecodeLeaseId("ls_osnQVCFZRZsEf63s9FS4kG")
	require.NoError(t, err)
}
