import Loadable from 'app/components/Loadable';
import { lazy } from 'react';
import { authRoles } from '../../auth/authRoles';

const Elasticseach = Loadable(lazy(() => import('./Elasticseach')));

const elasticRoutes = [
  { path: '/elasticsearch/default', element: <Elasticseach />, auth: authRoles.admin },
];

export default elasticRoutes;
