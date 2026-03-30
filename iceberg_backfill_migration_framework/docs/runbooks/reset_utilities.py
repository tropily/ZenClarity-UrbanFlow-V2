AUDIT_TABLE = "UrbanFlow_Migration_Audit"

def reset_slice(slice_id, db_hook, new_status='PENDING'):
    table = db_hook.get_conn().Table(AUDIT_TABLE)
    table.update_item(
        Key={'slice_id': slice_id},
        UpdateExpression='SET #s = :status',
        ExpressionAttributeNames={'#s': 'status'},
        ExpressionAttributeValues={':status': new_status}
    )
    print(f"Reset {slice_id} → {new_status}")  
