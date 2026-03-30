AUDIT_TABLE = "UrbanFlow_Migration_Audit"
def list_failed_slices(db_hook):
    table = db_hook.get_conn().Table(AUDIT_TABLE)
    response = table.scan(
    FilterExpression ='#s = :status',
    ExpressionAttributeNames={'#s': 'status'},
    ExpressionAttributeValues={':status': 'FAILED'}
 )
    items = response['Items']
    print(items)
    return items
failed = list_failed_slices(db_hook)
print(failed)
